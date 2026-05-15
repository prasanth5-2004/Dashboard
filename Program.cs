using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Client;
using System.Data.OleDb;
using System.Linq;
using System.Text.RegularExpressions;
using System.Timers;
#pragma warning disable CA1416

namespace MQTT_STA_Updater
{
    class Program
    {
        static ConfigRoot Config = new();
        static Dictionary<string, double> LastValues = new();
        static long LastEpochTime = 0;
        static readonly object lockObj = new();

        // ── DEDUPLICATION TRACKING ──
        // For water topic: tracks per-deviceId to allow Feed and Concentrate to be different payloads
        static Dictionary<string, Dictionary<long, DateTime>> ProcessedDeviceEpochs = new();
        static Dictionary<long, DateTime> ProcessedAirEpochs = new();

        // ── STANDARD BUFFER (modes 0-3): pairs water + air ──
        static Dictionary<long, BufferedData> DataBuffer = new();
        static Dictionary<long, FlushedRecord> FlushedSoloRecords = new();

        // ── DISTILLERY BUFFER (mode 4): waits for all 3 payloads ──
        // Keyed by epochTime. Accumulates FeedFlow, ConcentrateFlow, SPM.
        static Dictionary<long, DistilleryBuffer> DistilleryBufferMap = new();

        static System.Timers.Timer FlushTimer = new();

        static int FlushDelaySeconds = 900;   // 15 min wait for partner
        static int EpochMatchWindowSeconds = 1800;  // 30 min fuzzy match for late arrivals
        static int LateArrivalWindowSeconds = 3600; // 1 hour late-arrival UPDATE window

        // ── RECONNECTION: store options and client at class level ──────────────
        static MqttClientOptions? MqttOptions = null;
        static IMqttClient? MqttClient = null;

        static async Task Main(string[] args)
        {
            Console.Title = "MQTT → STA/Text + Access DB (Combined Rows)";
            LoadConfig();

            var WaterMapping = Config.water_parameters!
                .ToDictionary(k => k.Value.deviceId!, v => (v.Value.nmes!, v.Key));

            var AirMapping = Config.air_parameters!
                .ToDictionary(k => k.Value.deviceId!, v => (v.Value.nmes!, v.Key));

            FlushTimer = new System.Timers.Timer(60 * 1000);
            FlushTimer.Elapsed += FlushOldBufferedData;
            FlushTimer.AutoReset = true;
            FlushTimer.Start();

            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();

            mqttClient.ApplicationMessageReceivedAsync += async e =>
            {
                try
                {
                    string payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
                    string topic = e.ApplicationMessage.Topic;

                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"\n=== RECEIVED [{topic}] ===");
                    Console.ResetColor();

                    var (newValues, epochTime) = ParsePayload(payload);

                    if (newValues.Count == 0)
                    {
                        Console.WriteLine("⚠ No usable data in payload");
                        return;
                    }

                    if (epochTime == 0)
                    {
                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine("⚠ No timestamp in payload, using current time");
                        Console.ResetColor();
                        epochTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                    }

                    bool isWaterTopic = (topic == Config.mqtt!.topic_water);

                    // ── MODE 4: DISTILLERY ─────────────────────────────────────────────────
                    if (Config.output_mode == 4)
                    {
                        if (isWaterTopic)
                        {
                            // Each water payload carries exactly ONE deviceId (Feed OR Concentrate).
                            // Deduplicate per deviceId+epoch independently.
                            var distilleryWaterValues = newValues
                                .Where(x => x.Key == "Evaporator_Feed_Flow" || x.Key == "Evaporator_Concentrate_Flow")
                                .ToDictionary(x => x.Key, x => x.Value);

                            if (distilleryWaterValues.Count == 0)
                            {
                                Console.WriteLine("⚠ Distillery water payload: no Evaporator_Feed_Flow or Evaporator_Concentrate_Flow found");
                                return;
                            }

                            // Check duplicates for each deviceId separately
                            var nonDuplicates = new Dictionary<string, double>();
                            foreach (var kv in distilleryWaterValues)
                            {
                                if (IsDeviceDuplicate(kv.Key, epochTime))
                                {
                                    Console.ForegroundColor = ConsoleColor.Red;
                                    Console.WriteLine($"❌ DUPLICATE: {kv.Key} epoch={epochTime} — skipping");
                                    Console.ResetColor();
                                }
                                else
                                {
                                    nonDuplicates[kv.Key] = kv.Value;
                                }
                            }

                            if (nonDuplicates.Count == 0) return;

                            lock (lockObj)
                            {
                                foreach (var kv in nonDuplicates)
                                    LastValues[kv.Key] = kv.Value;
                                LastEpochTime = epochTime;
                            }

                            UpdateTextFileDistillery(Config.files!.display_text_distillery!, LastValues);
                            BufferDistillery(epochTime, nonDuplicates, isAir: false);
                        }
                        else // air topic
                        {
                            var distilleryAirValues = newValues
                                .Where(x => x.Key == "Boiler_Stack_SPM")
                                .ToDictionary(x => x.Key, x => x.Value);

                            if (distilleryAirValues.Count == 0)
                            {
                                Console.WriteLine("⚠ Distillery air payload: no Boiler_Stack_SPM found");
                                return;
                            }

                            if (IsDeviceDuplicate("Boiler_Stack_SPM", epochTime))
                            {
                                Console.ForegroundColor = ConsoleColor.Red;
                                Console.WriteLine($"❌ DUPLICATE: Boiler_Stack_SPM epoch={epochTime} — skipping");
                                Console.ResetColor();
                                return;
                            }

                            lock (lockObj)
                            {
                                foreach (var kv in distilleryAirValues)
                                    LastValues[kv.Key] = kv.Value;
                                LastEpochTime = epochTime;
                            }

                            UpdateTextFileDistillery(Config.files!.display_text_distillery!, LastValues);
                            BufferDistillery(epochTime, distilleryAirValues, isAir: true);
                        }

                        return; // mode 4 handled, exit handler
                    }

                    // ── MODES 0-3 ──────────────────────────────────────────────────────────
                    if (IsDuplicate(epochTime, isWaterTopic))
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"❌ DUPLICATE PAYLOAD DETECTED!");
                        Console.WriteLine($"   Topic: {topic}");
                        Console.WriteLine($"   Epoch: {epochTime} ({DateTimeOffset.FromUnixTimeSeconds(epochTime).LocalDateTime:yyyy-MM-dd HH:mm:ss})");
                        Console.WriteLine($"   → SKIPPING to prevent duplicate DB insert");
                        Console.ResetColor();
                        return;
                    }

                    lock (lockObj)
                    {
                        foreach (var kv in newValues)
                            LastValues[kv.Key] = kv.Value;
                        LastEpochTime = epochTime;
                    }

                    // ===== WATER (modes 0-3) =====
                    if (isWaterTopic)
                    {
                        var waterValues = newValues
                            .Where(x => x.Key.StartsWith("ETP_Outlet_"))
                            .ToDictionary(x => x.Key, x => x.Value);

                        if (waterValues.Count == 0)
                        {
                            Console.WriteLine("⚠ Water topic but no ETP_Outlet_* data");
                            return;
                        }

                        if (Config.output_mode == 0 || Config.output_mode == 2)
                            UpdateStaFile(Config.files!.water!, WaterMapping, LastValues);

                        if (Config.output_mode == 1 || Config.output_mode == 2)
                            UpdateTextFileUnit1(Config.files!.display_text_unit1!, LastValues, hasWater: true, hasAir: false);

                        if (Config.output_mode == 3)
                            UpdateTextFileUnit5(Config.files!.display_text_unit5!, LastValues, hasWater: true, hasAir: false);

                        BufferData(epochTime, waterValues, isWater: true);
                    }
                    // ===== AIR (modes 0-3) =====
                    else if (topic == Config.mqtt!.topic_air)
                    {
                        Console.WriteLine($"DEBUG: Received {newValues.Count} total values");
                        foreach (var kv in newValues)
                            Console.WriteLine($"  DeviceID: {kv.Key}");

                        if (Config.output_mode == 3)
                        {
                            var cogenerationValues = newValues
                                .Where(x => x.Key.StartsWith("Cogeneration_Stack_"))
                                .ToDictionary(x => x.Key, x => x.Value);

                            if (cogenerationValues.Count == 0)
                            {
                                Console.WriteLine("⚠ Unit 5 mode but no Cogeneration_Stack data");
                                return;
                            }

                            var normalizedValues = new Dictionary<string, double>();
                            foreach (var kv in cogenerationValues)
                            {
                                string normalizedKey = kv.Key.Replace("Cogeneration_Stack_", "Stack_");
                                normalizedValues[normalizedKey] = kv.Value;
                                LastValues[normalizedKey] = kv.Value;
                            }

                            UpdateStaFile(Config.files!.air!, AirMapping, LastValues);
                            UpdateTextFileUnit5(Config.files!.display_text_unit5!, LastValues, hasWater: false, hasAir: true);
                            BufferData(epochTime, normalizedValues, isWater: false);
                            return;
                        }

                        var normalizedAirValues = new Dictionary<string, double>();
                        foreach (var kv in newValues)
                        {
                            if (kv.Key.StartsWith("Cogeneration_Stack_"))
                            {
                                string normalizedKey = kv.Key.Replace("Cogeneration_Stack_", "Stack_");
                                normalizedAirValues[normalizedKey] = kv.Value;
                                LastValues[normalizedKey] = kv.Value;
                            }
                            else if (kv.Key.StartsWith("Stack_"))
                            {
                                normalizedAirValues[kv.Key] = kv.Value;
                            }
                        }

                        if (normalizedAirValues.Count == 0)
                        {
                            Console.WriteLine("⚠ Air topic but no Stack data");
                            return;
                        }

                        if (Config.output_mode == 0 || Config.output_mode == 2)
                            UpdateStaFile(Config.files!.air!, AirMapping, LastValues);

                        if (Config.output_mode == 1 || Config.output_mode == 2)
                            UpdateTextFileUnit1(Config.files!.display_text_unit1!, LastValues, hasWater: false, hasAir: true);

                        BufferData(epochTime, normalizedAirValues, isWater: false);
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("ERROR: " + ex.Message);
                    Console.WriteLine(ex.StackTrace);
                    Console.ResetColor();
                }
            };

            // ── CHANGE 1: Store options at class level, add KeepAlive + CleanSession ──
            MqttOptions = new MqttClientOptionsBuilder()
                .WithTcpServer(Config.mqtt!.broker, Config.mqtt.port)
                .WithClientId("STA-DB-" + Environment.MachineName)
                .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
                .WithCleanSession(false)
                .Build();

            MqttClient = mqttClient;

            // ── CHANGE 2: Reconnection handler ───────────────────────────────────────
            mqttClient.DisconnectedAsync += async e =>
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\n⚠ MQTT DISCONNECTED: {e.Exception?.Message ?? "unknown reason"}");
                Console.WriteLine("  Attempting reconnect in 5 seconds...");
                Console.ResetColor();

                await Task.Delay(5000);

                int attempt = 0;
                while (true)
                {
                    try
                    {
                        attempt++;
                        Console.WriteLine($"  Reconnect attempt #{attempt}...");
                        await mqttClient.ConnectAsync(MqttOptions);

                        // Re-subscribe after reconnect
                        await mqttClient.SubscribeAsync(Config.mqtt.topic_water!);
                        await mqttClient.SubscribeAsync(Config.mqtt.topic_air!);

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"✓ Reconnected successfully on attempt #{attempt}");
                        Console.ResetColor();
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"  ❌ Reconnect failed: {ex.Message}. Retrying in 10s...");
                        Console.ResetColor();
                        await Task.Delay(10000);
                    }
                }
            };

            await mqttClient.ConnectAsync(MqttOptions);
            await mqttClient.SubscribeAsync(Config.mqtt.topic_water!);
            await mqttClient.SubscribeAsync(Config.mqtt.topic_air!);

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("✓ MQTT Connected");
            string modeText = Config.output_mode switch
            {
                0 => "STA Files Only",
                1 => "Text File (Unit 1) Only",
                2 => "STA + Text File (Unit 1)",
                3 => "Text File (Unit 5) Only - Cogeneration",
                4 => "Text File (Distillery) Only",
                _ => "Unknown"
            };
            Console.WriteLine($"✓ Output Mode: {modeText}");
            Console.WriteLine("✓ EpochTime tracking enabled");
            Console.WriteLine($"✓ Data buffering enabled (flush delay: {FlushDelaySeconds}s = {FlushDelaySeconds / 60} min)");
            Console.WriteLine($"✓ Epoch match window: ±{EpochMatchWindowSeconds}s");
            Console.WriteLine($"✓ Late arrival UPDATE window: {LateArrivalWindowSeconds}s = {LateArrivalWindowSeconds / 60} min");
            Console.WriteLine("✓ Duplicate payload detection enabled");
            Console.WriteLine("✓ Auto-reconnect enabled (keepalive=30s, retry every 10s)");
            if (Config.output_mode == 4)
                Console.WriteLine("✓ Distillery mode: waiting for Feed Flow + Concentrate Flow + SPM per epoch");
            Console.WriteLine("✓ Waiting for data...\n");
            Console.ResetColor();

            await Task.Delay(-1);
        }

        // ─────────────────────────────────────────────────────────────────────
        //  IsDuplicate — for modes 0-3 (per topic)
        // ─────────────────────────────────────────────────────────────────────
        static bool IsDuplicate(long epochTime, bool isWater)
        {
            lock (lockObj)
            {
                var processedDict = isWater
                    ? GetDeviceDict("__water__")
                    : ProcessedAirEpochs;

                var twoHoursAgo = DateTime.Now.AddHours(-2);
                var toRemove = processedDict.Where(kv => kv.Value < twoHoursAgo).Select(kv => kv.Key).ToList();
                foreach (var key in toRemove) processedDict.Remove(key);

                if (processedDict.ContainsKey(epochTime)) return true;
                processedDict[epochTime] = DateTime.Now;
                return false;
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        //  IsDeviceDuplicate — for mode 4 (per deviceId+epoch)
        // ─────────────────────────────────────────────────────────────────────
        static bool IsDeviceDuplicate(string deviceId, long epochTime)
        {
            lock (lockObj)
            {
                var dict = GetDeviceDict(deviceId);

                var twoHoursAgo = DateTime.Now.AddHours(-2);
                var toRemove = dict.Where(kv => kv.Value < twoHoursAgo).Select(kv => kv.Key).ToList();
                foreach (var key in toRemove) dict.Remove(key);

                if (dict.ContainsKey(epochTime)) return true;
                dict[epochTime] = DateTime.Now;
                return false;
            }
        }

        static Dictionary<long, DateTime> GetDeviceDict(string deviceId)
        {
            if (!ProcessedDeviceEpochs.TryGetValue(deviceId, out var dict))
            {
                dict = new Dictionary<long, DateTime>();
                ProcessedDeviceEpochs[deviceId] = dict;
            }
            return dict;
        }

        static void LoadConfig()
        {
            string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config.json");
            if (!File.Exists(path))
            {
                Console.WriteLine("config.json NOT FOUND");
                Environment.Exit(1);
            }
            Config = JsonSerializer.Deserialize<ConfigRoot>(File.ReadAllText(path))!;

            if (Config.output_mode < 0 || Config.output_mode > 4)
            {
                Console.WriteLine("Invalid output_mode in config. Must be 0-4.");
                Environment.Exit(1);
            }
        }

        static (Dictionary<string, double> values, long epochTime) ParsePayload(string raw)
        {
            var result = new Dictionary<string, double>();
            long epochTime = 0;

            if (string.IsNullOrWhiteSpace(raw)) return (result, epochTime);

            try
            {
                using var doc = JsonDocument.Parse(raw);

                foreach (var item in doc.RootElement.EnumerateArray())
                {
                    string deviceId = item.GetProperty("deviceId").GetString() ?? "";

                    if (item.TryGetProperty("timestamp", out var tsElement))
                        epochTime = ExtractEpochTime(tsElement);
                    else if (item.TryGetProperty("ts", out tsElement))
                        epochTime = ExtractEpochTime(tsElement);
                    else if (item.TryGetProperty("time", out tsElement))
                        epochTime = ExtractEpochTime(tsElement);

                    foreach (var p in item.GetProperty("params").EnumerateArray())
                    {
                        double val;
                        var valueElement = p.GetProperty("value");

                        if (valueElement.ValueKind == JsonValueKind.Number)
                            val = valueElement.GetDouble();
                        else if (valueElement.ValueKind == JsonValueKind.String)
                        {
                            if (!double.TryParse(valueElement.GetString(),
                                System.Globalization.NumberStyles.Any,
                                System.Globalization.CultureInfo.InvariantCulture,
                                out val)) continue;
                        }
                        else continue;

                        result[deviceId] = val;
                        Console.WriteLine($"  {deviceId} = {val}");

                        if (epochTime == 0 && p.TryGetProperty("timestamp", out var paramTsElement))
                            epochTime = ExtractEpochTime(paramTsElement);
                    }
                }

                if (epochTime > 0)
                {
                    var dt = DateTimeOffset.FromUnixTimeSeconds(epochTime).LocalDateTime;
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"  📅 Payload Timestamp: {dt:yyyy-MM-dd HH:mm:ss} (Epoch: {epochTime})");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Parse error: " + ex.Message);
            }

            return (result, epochTime);
        }

        static long ExtractEpochTime(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Number)
            {
                long ts = element.GetInt64();
                return ts > 10000000000 ? ts / 1000 : ts;
            }
            else if (element.ValueKind == JsonValueKind.String)
            {
                if (long.TryParse(element.GetString(), out var ts))
                    return ts > 10000000000 ? ts / 1000 : ts;
            }
            return 0;
        }

        // ═════════════════════════════════════════════════════════════════════
        //  MODE 4 — DISTILLERY BUFFERING
        //  Three payloads must all arrive before we insert one row.
        //  Matching: EXACT epoch for the live buffer, fuzzy for late arrivals.
        // ═════════════════════════════════════════════════════════════════════
        static void BufferDistillery(long epochTime, Dictionary<string, double> values, bool isAir)
        {
            lock (lockObj)
            {
                // ── PATH 1: Exact-epoch match in the active distillery buffer ──────────
                if (DistilleryBufferMap.TryGetValue(epochTime, out var existing))
                {
                    foreach (var kv in values)
                    {
                        existing.Values[kv.Key] = kv.Value;
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine($"  🔗 Distillery: merged {kv.Key} into buffer (epoch={epochTime})");
                        Console.ResetColor();
                    }

                    TryFlushDistillery(epochTime, existing);
                    return;
                }

                // ── PATH 2: Fuzzy match in active buffer (handles minor clock drift) ──
                // Only for the active buffer — look for an entry within EpochMatchWindowSeconds
                // that is still missing what we just received.
                DistilleryBuffer? fuzzyMatch = null;
                long fuzzyKey = 0;

                foreach (var kv in DistilleryBufferMap)
                {
                    long diff = Math.Abs(kv.Key - epochTime);
                    if (diff > 0 && diff <= EpochMatchWindowSeconds)
                    {
                        // Check it is actually missing at least one of our values
                        bool missingAny = values.Keys.Any(k => !kv.Value.Values.ContainsKey(k));
                        if (missingAny)
                        {
                            if (fuzzyMatch == null || diff < Math.Abs(fuzzyKey - epochTime))
                            {
                                fuzzyMatch = kv.Value;
                                fuzzyKey = kv.Key;
                            }
                        }
                    }
                }

                if (fuzzyMatch != null)
                {
                    long diff = Math.Abs(fuzzyKey - epochTime);
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"  🔗 Distillery: fuzzy match (diff={diff}s) merged into buffer (bufferEpoch={fuzzyKey})");
                    Console.ResetColor();
                    foreach (var kv in values)
                        fuzzyMatch.Values[kv.Key] = kv.Value;
                    TryFlushDistillery(fuzzyKey, fuzzyMatch);
                    return;
                }

                // ── PATH 3: Late arrival — row was already flushed solo ───────────────
                // Find a flushed solo record missing exactly what we just received.
                DistilleryFlushedRecord? flushedMatch = null;
                long flushedKey = 0;

                foreach (var kv in DistilleryFlushedRecords)
                {
                    long diff = Math.Abs(kv.Key - epochTime);
                    if (diff <= EpochMatchWindowSeconds)
                    {
                        bool missingAny = values.Keys.Any(k => !kv.Value.Values.ContainsKey(k));
                        if (missingAny)
                        {
                            if (flushedMatch == null || diff < Math.Abs(flushedKey - epochTime))
                            {
                                flushedMatch = kv.Value;
                                flushedKey = kv.Key;
                            }
                        }
                    }
                }

                if (flushedMatch != null)
                {
                    long diff = Math.Abs(flushedKey - epochTime);
                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine($"\n  ⚡ LATE ARRIVAL (Distillery): arrived (epoch={epochTime})");
                    Console.WriteLine($"     Matched flushed solo record at epoch={flushedKey} (diff={diff}s)");
                    Console.WriteLine($"     → Performing UPDATE on existing DB row");
                    Console.ResetColor();

                    foreach (var kv in values)
                        flushedMatch.Values[kv.Key] = kv.Value;

                    UpdateDistilleryRow(flushedMatch.Values, flushedKey);
                    DistilleryFlushedRecords[flushedKey] = flushedMatch;
                    return;
                }

                // ── PATH 4: No match — new buffer entry ───────────────────────────────
                var newBuf = new DistilleryBuffer
                {
                    EpochTime = epochTime,
                    ReceivedAt = DateTime.Now
                };
                foreach (var kv in values)
                    newBuf.Values[kv.Key] = kv.Value;

                DistilleryBufferMap[epochTime] = newBuf;

                string arrivedKeys = string.Join(", ", values.Keys);
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"  🔵 Distillery buffered [{arrivedKeys}] (epoch={epochTime}) — waiting for remaining payloads...");
                Console.ResetColor();
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        //  TryFlushDistillery — insert once all 3 values have arrived
        // ─────────────────────────────────────────────────────────────────────
        static Dictionary<long, DistilleryFlushedRecord> DistilleryFlushedRecords = new();

        static void TryFlushDistillery(long epochKey, DistilleryBuffer buf)
        {
            // Must be called inside lockObj
            bool hasFeed = buf.Values.ContainsKey("Evaporator_Feed_Flow");
            bool hasConc = buf.Values.ContainsKey("Evaporator_Concentrate_Flow");
            bool hasSPM = buf.Values.ContainsKey("Boiler_Stack_SPM");

            string status = $"Feed={hasFeed}, Conc={hasConc}, SPM={hasSPM}";
            Console.WriteLine($"  Distillery buffer status (epoch={epochKey}): {status}");

            if (hasFeed && hasConc && hasSPM)
            {
                double waited = (DateTime.Now - buf.ReceivedAt).TotalSeconds;
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"  ✓ ALL 3 payloads received — inserting combined row. Waited {waited:0.0}s.");
                Console.ResetColor();

                InsertDistillery(buf.Values, epochKey);
                DistilleryBufferMap.Remove(epochKey);
            }
            else
            {
                // Not yet complete — check if we should also register as partial flushed record
                // (timer will handle timeout flushing separately)
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        //  UpdateDistilleryRow — UPDATE an already-inserted distillery row
        // ─────────────────────────────────────────────────────────────────────
        static void UpdateDistilleryRow(Dictionary<string, double> values, long epochTime)
        {
            try
            {
                using var con = new OleDbConnection(Config.database!.connection_string);
                con.Open();

                DateTime measurementTime = epochTime > 0
                    ? DateTimeOffset.FromUnixTimeSeconds(epochTime).LocalDateTime
                    : DateTime.Now;

                // Build SET clause dynamically: only update columns that have new data
                var setClauses = new List<string>();
                var parameters = new List<(string name, OleDbType type, object val)>();

                if (values.TryGetValue("Evaporator_Feed_Flow", out var feedFlow))
                {
                    setClauses.Add("[DeviceID 1] = ?, [Evaportar Feed Flow] = ?");
                    parameters.Add(("D1", OleDbType.VarChar, "Evaporator_Feed_Flow"));
                    parameters.Add(("FeedFlow", OleDbType.Double, feedFlow));
                }

                if (values.TryGetValue("Evaporator_Concentrate_Flow", out var concFlow))
                {
                    setClauses.Add("[DeviceID 2] = ?, [Evaportar Concentrate Flow] = ?");
                    parameters.Add(("D2", OleDbType.VarChar, "Evaporator_Concentrate_Flow"));
                    parameters.Add(("ConcFlow", OleDbType.Double, concFlow));
                }

                if (values.TryGetValue("Boiler_Stack_SPM", out var spm))
                {
                    setClauses.Add("[DeviceID 3] = ?, [SPM] = ?");
                    parameters.Add(("D3", OleDbType.VarChar, "Boiler_Stack_SPM"));
                    parameters.Add(("SPM", OleDbType.Double, spm));
                }

                if (setClauses.Count == 0)
                {
                    Console.WriteLine("⚠ UPDATE skipped: no new values to set");
                    return;
                }

                string sql = $"UPDATE RawData SET {string.Join(", ", setClauses)} WHERE [EpochTime] = ?";

                try
                {
                    using var cmd = new OleDbCommand(sql, con);
                    foreach (var (name, type, val) in parameters)
                        cmd.Parameters.Add(name, type).Value = val;
                    cmd.Parameters.Add("Epoch", OleDbType.Double).Value = (double)epochTime;

                    int rows = cmd.ExecuteNonQuery();
                    LogUpdateResult(rows, epochTime, measurementTime, "Distillery (late)");
                    if (rows > 0) return;
                }
                catch (OleDbException) { /* fall through to DateTime fallback */ }

                // Fallback: match by DateTime
                string sqlDt = $"UPDATE RawData SET {string.Join(", ", setClauses)} WHERE [DateTime] = ?";
                using var cmd2 = new OleDbCommand(sqlDt, con);
                foreach (var (name, type, val) in parameters)
                    cmd2.Parameters.Add(name, type).Value = val;
                cmd2.Parameters.Add("DT", OleDbType.Date).Value = measurementTime;

                int rows2 = cmd2.ExecuteNonQuery();
                LogUpdateResult(rows2, epochTime, measurementTime, "Distillery (late, DateTime fallback)");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ DB UPDATE (Distillery) FAILED: {ex.Message}");
                Console.ResetColor();
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        //  CHANGE 3: FlushOldBufferedData — timer fires every 60s
        //  Wrapped entire body in try-catch to prevent silent timer death
        // ─────────────────────────────────────────────────────────────────────
        static void FlushOldBufferedData(object? sender, ElapsedEventArgs e)
        {
            try
            {
                lock (lockObj)
                {
                    var now = DateTime.Now;

                    // ── Flush timed-out standard buffer entries (modes 0-3) ──────────────
                    var toFlush = DataBuffer
                        .Where(kv => (now - kv.Value.ReceivedAt).TotalSeconds >= FlushDelaySeconds)
                        .Select(kv => kv.Key).ToList();

                    foreach (var epochTime in toFlush)
                    {
                        var buffer = DataBuffer[epochTime];
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"\n⚠ TIMEOUT ({FlushDelaySeconds}s): No partner for epoch {epochTime}");
                        Console.WriteLine($"  HasWater={buffer.HasWater}, HasAir={buffer.HasAir}");
                        Console.WriteLine($"  Inserting solo record. Late arrival can UPDATE for {LateArrivalWindowSeconds}s.");
                        Console.ResetColor();

                        buffer.EpochTime = buffer.FirstArrivalEpoch;
                        FlushBufferedData(buffer.EpochTime, buffer);
                        DataBuffer.Remove(epochTime);
                    }

                    // ── Prune expired standard solo records ──────────────────────────────
                    var toExpire = FlushedSoloRecords
                        .Where(kv => (now - kv.Value.FlushedAt).TotalSeconds >= LateArrivalWindowSeconds)
                        .Select(kv => kv.Key).ToList();
                    foreach (var key in toExpire)
                    {
                        Console.WriteLine($"  🗑 Expired late-arrival window for epoch={key}");
                        FlushedSoloRecords.Remove(key);
                    }

                    // ── Flush timed-out distillery buffer entries (mode 4) ───────────────
                    var distilleryToFlush = DistilleryBufferMap
                        .Where(kv => (now - kv.Value.ReceivedAt).TotalSeconds >= FlushDelaySeconds)
                        .Select(kv => kv.Key).ToList();

                    foreach (var epochTime in distilleryToFlush)
                    {
                        var buf = DistilleryBufferMap[epochTime];
                        bool hasFeed = buf.Values.ContainsKey("Evaporator_Feed_Flow");
                        bool hasConc = buf.Values.ContainsKey("Evaporator_Concentrate_Flow");
                        bool hasSPM = buf.Values.ContainsKey("Boiler_Stack_SPM");

                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"\n⚠ DISTILLERY TIMEOUT ({FlushDelaySeconds}s): epoch={epochTime}");
                        Console.WriteLine($"  Feed={hasFeed}, Conc={hasConc}, SPM={hasSPM}");
                        Console.WriteLine($"  Inserting partial row. Late arrival can UPDATE for {LateArrivalWindowSeconds}s.");
                        Console.ResetColor();

                        InsertDistillery(buf.Values, epochTime);

                        // Register as flushed solo so late arrivals can UPDATE
                        var flushed = new DistilleryFlushedRecord
                        {
                            EpochTime = epochTime,
                            FlushedAt = now
                        };
                        foreach (var kv in buf.Values)
                            flushed.Values[kv.Key] = kv.Value;

                        DistilleryFlushedRecords[epochTime] = flushed;
                        DistilleryBufferMap.Remove(epochTime);
                    }

                    // ── Prune expired distillery flushed records ─────────────────────────
                    var distilleryToExpire = DistilleryFlushedRecords
                        .Where(kv => (now - kv.Value.FlushedAt).TotalSeconds >= LateArrivalWindowSeconds)
                        .Select(kv => kv.Key).ToList();
                    foreach (var key in distilleryToExpire)
                    {
                        Console.WriteLine($"  🗑 Distillery: Expired late-arrival window for epoch={key}");
                        DistilleryFlushedRecords.Remove(key);
                    }
                }
            }
            catch (Exception ex)
            {
                // Without this catch, any exception here silently kills the timer forever
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ ERROR in flush timer: {ex.Message}");
                Console.ResetColor();
            }
        }

        // ═════════════════════════════════════════════════════════════════════
        //  MODES 0-3 BUFFER LOGIC (unchanged from original)
        // ═════════════════════════════════════════════════════════════════════
        static void BufferData(long epochTime, Dictionary<string, double> values, bool isWater)
        {
            lock (lockObj)
            {
                string side = isWater ? "WATER" : "AIR";

                // PATH 1: Exact-epoch match in active buffer
                BufferedData? matchedBuffer = null;
                long matchedKey = 0;

                foreach (var kv in DataBuffer)
                {
                    var existing = kv.Value;
                    bool waitingForThisSide = isWater ? !existing.HasWater : !existing.HasAir;
                    if (existing.EpochTime == epochTime && waitingForThisSide)
                    {
                        matchedBuffer = existing;
                        matchedKey = kv.Key;
                        break;
                    }
                }

                if (matchedBuffer != null)
                {
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"  🔗 {side} matched to existing buffer (exact epoch={epochTime})");
                    Console.ResetColor();

                    if (isWater)
                    {
                        foreach (var kv in values) matchedBuffer.WaterData[kv.Key] = kv.Value;
                        matchedBuffer.HasWater = true;
                        matchedBuffer.WaterEpoch = epochTime;
                    }
                    else
                    {
                        foreach (var kv in values) matchedBuffer.AirData[kv.Key] = kv.Value;
                        matchedBuffer.HasAir = true;
                        matchedBuffer.AirEpoch = epochTime;
                    }

                    double waited = (DateTime.Now - matchedBuffer.ReceivedAt).TotalSeconds;
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"  ✓ COMPLETE: Water+Air paired. Waited {waited:0.0}s.");
                    Console.ResetColor();

                    matchedBuffer.EpochTime = matchedBuffer.FirstArrivalEpoch;
                    FlushBufferedData(matchedBuffer.EpochTime, matchedBuffer);
                    DataBuffer.Remove(matchedKey);
                    return;
                }

                // PATH 2: Late arrival — already flushed solo
                FlushedRecord? flushedMatch = null;
                long flushedMatchKey = 0;

                foreach (var kv in FlushedSoloRecords)
                {
                    var rec = kv.Value;
                    long diff = Math.Abs(rec.EpochTime - epochTime);
                    bool missingThisSide = isWater ? !rec.HasWater : !rec.HasAir;

                    if (diff <= EpochMatchWindowSeconds && missingThisSide)
                    {
                        if (flushedMatch == null || diff < Math.Abs(flushedMatch.EpochTime - epochTime))
                        {
                            flushedMatch = rec;
                            flushedMatchKey = kv.Key;
                        }
                    }
                }

                if (flushedMatch != null)
                {
                    long diff = Math.Abs(flushedMatch.EpochTime - epochTime);
                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine($"\n  ⚡ LATE ARRIVAL: {side} (epoch={epochTime}) matched flushed record (epoch={flushedMatch.EpochTime}, diff={diff}s) → UPDATE");
                    Console.ResetColor();

                    if (isWater)
                    {
                        foreach (var kv in values) flushedMatch.WaterData[kv.Key] = kv.Value;
                        flushedMatch.HasWater = true;
                    }
                    else
                    {
                        foreach (var kv in values) flushedMatch.AirData[kv.Key] = kv.Value;
                        flushedMatch.HasAir = true;
                    }

                    var combined = new Dictionary<string, double>();
                    foreach (var kv in flushedMatch.WaterData) combined[kv.Key] = kv.Value;
                    foreach (var kv in flushedMatch.AirData) combined[kv.Key] = kv.Value;

                    UpdateExistingRow(combined, flushedMatch.EpochTime, flushedMatch.HasWater, flushedMatch.HasAir);
                    FlushedSoloRecords[flushedMatchKey] = flushedMatch;
                    return;
                }

                // PATH 3: No match — new buffer entry
                if (DataBuffer.TryGetValue(epochTime, out var existingEntry))
                {
                    bool alreadyHasThisSide = isWater ? existingEntry.HasWater : existingEntry.HasAir;
                    if (alreadyHasThisSide)
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"  ⚠ DUPLICATE {side} payload for epoch={epochTime} — overwriting values");
                        Console.ResetColor();
                        if (isWater)
                            foreach (var kv in values) existingEntry.WaterData[kv.Key] = kv.Value;
                        else
                            foreach (var kv in values) existingEntry.AirData[kv.Key] = kv.Value;
                        return;
                    }
                }

                var newEntry = new BufferedData
                {
                    EpochTime = epochTime,
                    ReceivedAt = DateTime.Now,
                    FirstArrivalEpoch = epochTime,
                    WaterEpoch = isWater ? epochTime : 0,
                    AirEpoch = isWater ? 0 : epochTime
                };

                if (isWater)
                {
                    foreach (var kv in values) newEntry.WaterData[kv.Key] = kv.Value;
                    newEntry.HasWater = true;
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"  🔵 WATER buffered (epoch={epochTime}) — waiting {FlushDelaySeconds}s for AIR...");
                }
                else
                {
                    foreach (var kv in values) newEntry.AirData[kv.Key] = kv.Value;
                    newEntry.HasAir = true;
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"  🟢 AIR buffered (epoch={epochTime}) — waiting {FlushDelaySeconds}s for WATER...");
                }
                Console.ResetColor();

                DataBuffer[epochTime] = newEntry;
            }
        }

        static void FlushBufferedData(long epochTime, BufferedData buffer)
        {
            var combinedValues = new Dictionary<string, double>();
            foreach (var kv in buffer.WaterData) combinedValues[kv.Key] = kv.Value;
            foreach (var kv in buffer.AirData) combinedValues[kv.Key] = kv.Value;

            InsertCombined(combinedValues, epochTime, buffer.HasWater, buffer.HasAir);

            if (!(buffer.HasWater && buffer.HasAir))
            {
                var flushed = new FlushedRecord
                {
                    EpochTime = epochTime,
                    FlushedAt = DateTime.Now,
                    HasWater = buffer.HasWater,
                    HasAir = buffer.HasAir
                };
                foreach (var kv in buffer.WaterData) flushed.WaterData[kv.Key] = kv.Value;
                foreach (var kv in buffer.AirData) flushed.AirData[kv.Key] = kv.Value;

                FlushedSoloRecords[epochTime] = flushed;

                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"  📋 Solo record registered for late-arrival UPDATE (epoch={epochTime}, window={LateArrivalWindowSeconds}s)");
                Console.ResetColor();
            }
        }

        static void UpdateExistingRow(Dictionary<string, double> values,
                                      long epochTime, bool hasWater, bool hasAir)
        {
            try
            {
                using var con = new OleDbConnection(Config.database!.connection_string);
                con.Open();

                DateTime measurementTime = epochTime > 0
                    ? DateTimeOffset.FromUnixTimeSeconds(epochTime).LocalDateTime
                    : DateTime.Now;

                string sql;

                if (hasWater && !hasAir)
                {
                    sql = @"
UPDATE RawData SET
  [DeviceID 1]=?,[DeviceID 2]=?,[DeviceID 3]=?,[DeviceID 4]=?,[DeviceID 5]=?,
  Flow=?,pH=?,TSS=?,BOD=?,COD=?
WHERE [EpochTime]=?";
                    try
                    {
                        using var cmd = new OleDbCommand(sql, con);
                        cmd.Parameters.Add("D1", OleDbType.VarChar).Value = "ETP_Outlet_Flow";
                        cmd.Parameters.Add("D2", OleDbType.VarChar).Value = "ETP_Outlet_pH";
                        cmd.Parameters.Add("D3", OleDbType.VarChar).Value = "ETP_Outlet_TSS";
                        cmd.Parameters.Add("D4", OleDbType.VarChar).Value = "ETP_Outlet_BOD";
                        cmd.Parameters.Add("D5", OleDbType.VarChar).Value = "ETP_Outlet_COD";
                        cmd.Parameters.Add("Flow", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_Flow", out var f) ? f : (object)DBNull.Value;
                        cmd.Parameters.Add("pH", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_pH", out var ph) ? ph : (object)DBNull.Value;
                        cmd.Parameters.Add("TSS", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_TSS", out var tss) ? tss : (object)DBNull.Value;
                        cmd.Parameters.Add("BOD", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_BOD", out var bod) ? bod : (object)DBNull.Value;
                        cmd.Parameters.Add("COD", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_COD", out var cod) ? cod : (object)DBNull.Value;
                        cmd.Parameters.Add("Ep", OleDbType.Double).Value = (double)epochTime;
                        int rows = cmd.ExecuteNonQuery();
                        LogUpdateResult(rows, epochTime, measurementTime, "WATER (late)");
                        if (rows > 0) return;
                    }
                    catch (OleDbException) { }

                    sql = sql.Replace("WHERE [EpochTime]=?", "WHERE [DateTime]=?");
                    using var cmd2 = new OleDbCommand(sql, con);
                    cmd2.Parameters.Add("D1", OleDbType.VarChar).Value = "ETP_Outlet_Flow";
                    cmd2.Parameters.Add("D2", OleDbType.VarChar).Value = "ETP_Outlet_pH";
                    cmd2.Parameters.Add("D3", OleDbType.VarChar).Value = "ETP_Outlet_TSS";
                    cmd2.Parameters.Add("D4", OleDbType.VarChar).Value = "ETP_Outlet_BOD";
                    cmd2.Parameters.Add("D5", OleDbType.VarChar).Value = "ETP_Outlet_COD";
                    cmd2.Parameters.Add("Flow", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_Flow", out var f2) ? f2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("pH", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_pH", out var ph2) ? ph2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("TSS", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_TSS", out var tss2) ? tss2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("BOD", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_BOD", out var bod2) ? bod2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("COD", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_COD", out var cod2) ? cod2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("DT", OleDbType.Date).Value = measurementTime;
                    LogUpdateResult(cmd2.ExecuteNonQuery(), epochTime, measurementTime, "WATER (late, DateTime fallback)");
                }
                else if (hasAir && !hasWater)
                {
                    sql = @"
UPDATE RawData SET
  [DeviceID 6]=?,[DeviceID 7]=?,[DeviceID 8]=?,SOX=?,NOX=?,SPM=?
WHERE [EpochTime]=?";
                    try
                    {
                        using var cmd = new OleDbCommand(sql, con);
                        cmd.Parameters.Add("D6", OleDbType.VarChar).Value = "Stack_SO2";
                        cmd.Parameters.Add("D7", OleDbType.VarChar).Value = "Stack_NOx";
                        cmd.Parameters.Add("D8", OleDbType.VarChar).Value = "Stack_SPM";
                        cmd.Parameters.Add("SOX", OleDbType.Double).Value = values.TryGetValue("Stack_SO2", out var so2) ? so2 : (object)DBNull.Value;
                        cmd.Parameters.Add("NOX", OleDbType.Double).Value = values.TryGetValue("Stack_NOx", out var nox) ? nox : (object)DBNull.Value;
                        cmd.Parameters.Add("SPM", OleDbType.Double).Value = values.TryGetValue("Stack_SPM", out var spm) ? spm : (object)DBNull.Value;
                        cmd.Parameters.Add("Ep", OleDbType.Double).Value = (double)epochTime;
                        int rows = cmd.ExecuteNonQuery();
                        LogUpdateResult(rows, epochTime, measurementTime, "AIR (late)");
                        if (rows > 0) return;
                    }
                    catch (OleDbException) { }

                    sql = sql.Replace("WHERE [EpochTime]=?", "WHERE [DateTime]=?");
                    using var cmd2 = new OleDbCommand(sql, con);
                    cmd2.Parameters.Add("D6", OleDbType.VarChar).Value = "Stack_SO2";
                    cmd2.Parameters.Add("D7", OleDbType.VarChar).Value = "Stack_NOx";
                    cmd2.Parameters.Add("D8", OleDbType.VarChar).Value = "Stack_SPM";
                    cmd2.Parameters.Add("SOX", OleDbType.Double).Value = values.TryGetValue("Stack_SO2", out var so22) ? so22 : (object)DBNull.Value;
                    cmd2.Parameters.Add("NOX", OleDbType.Double).Value = values.TryGetValue("Stack_NOx", out var nox2) ? nox2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("SPM", OleDbType.Double).Value = values.TryGetValue("Stack_SPM", out var spm2) ? spm2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("DT", OleDbType.Date).Value = measurementTime;
                    LogUpdateResult(cmd2.ExecuteNonQuery(), epochTime, measurementTime, "AIR (late, DateTime fallback)");
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ DB UPDATE FAILED: {ex.Message}");
                Console.ResetColor();
            }
        }

        static void LogUpdateResult(int rowsAffected, long epochTime, DateTime dt, string label)
        {
            if (rowsAffected > 0)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"  ✓ DB UPDATE OK ({label}) - Time: {dt:yyyy-MM-dd HH:mm:ss}, Epoch: {epochTime}, Rows: {rowsAffected}");
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"  ❌ DB UPDATE found 0 rows for epoch={epochTime} / time={dt:yyyy-MM-dd HH:mm:ss}");
            }
            Console.ResetColor();
        }

        // ─────────────────────────────────────────────────────────────────────
        //  InsertCombined — modes 0-3 (unchanged schema)
        // ─────────────────────────────────────────────────────────────────────
        static void InsertCombined(Dictionary<string, double> values,
                                   long epochTime, bool hasWater, bool hasAir)
        {
            try
            {
                using var con = new OleDbConnection(Config.database!.connection_string);
                con.Open();

                DateTime measurementTime = epochTime > 0
                    ? DateTimeOffset.FromUnixTimeSeconds(epochTime).LocalDateTime
                    : DateTime.Now;

                try
                {
                    string sql = @"
INSERT INTO RawData
([DateTime],[EpochTime],
 [DeviceID 1],[DeviceID 2],[DeviceID 3],[DeviceID 4],[DeviceID 5],
 [DeviceID 6],[DeviceID 7],[DeviceID 8],
 Flow,pH,TSS,BOD,COD,SOX,NOX,SPM)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                    using var cmd = new OleDbCommand(sql, con);
                    cmd.Parameters.Add("DT", OleDbType.Date).Value = measurementTime;
                    cmd.Parameters.Add("Ep", OleDbType.Double).Value = (double)epochTime;
                    cmd.Parameters.Add("D1", OleDbType.VarChar).Value = hasWater ? "ETP_Outlet_Flow" : (object)DBNull.Value;
                    cmd.Parameters.Add("D2", OleDbType.VarChar).Value = hasWater ? "ETP_Outlet_pH" : (object)DBNull.Value;
                    cmd.Parameters.Add("D3", OleDbType.VarChar).Value = hasWater ? "ETP_Outlet_TSS" : (object)DBNull.Value;
                    cmd.Parameters.Add("D4", OleDbType.VarChar).Value = hasWater ? "ETP_Outlet_BOD" : (object)DBNull.Value;
                    cmd.Parameters.Add("D5", OleDbType.VarChar).Value = hasWater ? "ETP_Outlet_COD" : (object)DBNull.Value;
                    cmd.Parameters.Add("D6", OleDbType.VarChar).Value = hasAir ? "Stack_SO2" : (object)DBNull.Value;
                    cmd.Parameters.Add("D7", OleDbType.VarChar).Value = hasAir ? "Stack_NOx" : (object)DBNull.Value;
                    cmd.Parameters.Add("D8", OleDbType.VarChar).Value = hasAir ? "Stack_SPM" : (object)DBNull.Value;
                    cmd.Parameters.Add("Flow", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_Flow", out var f) ? f : (object)DBNull.Value;
                    cmd.Parameters.Add("pH", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_pH", out var ph) ? ph : (object)DBNull.Value;
                    cmd.Parameters.Add("TSS", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_TSS", out var tss) ? tss : (object)DBNull.Value;
                    cmd.Parameters.Add("BOD", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_BOD", out var bod) ? bod : (object)DBNull.Value;
                    cmd.Parameters.Add("COD", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_COD", out var cod) ? cod : (object)DBNull.Value;
                    cmd.Parameters.Add("SOX", OleDbType.Double).Value = values.TryGetValue("Stack_SO2", out var so2) ? so2 : (object)DBNull.Value;
                    cmd.Parameters.Add("NOX", OleDbType.Double).Value = values.TryGetValue("Stack_NOx", out var nox) ? nox : (object)DBNull.Value;
                    cmd.Parameters.Add("SPM", OleDbType.Double).Value = values.TryGetValue("Stack_SPM", out var spm) ? spm : (object)DBNull.Value;
                    cmd.ExecuteNonQuery();

                    string dataType = (hasWater && hasAir) ? "COMBINED" : hasWater ? "Water Only" : "Air Only";
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"✓ DB INSERT OK ({dataType}) - Time: {measurementTime:yyyy-MM-dd HH:mm:ss}, Epoch: {epochTime}");
                    Console.ResetColor();
                }
                catch (OleDbException ex) when (ex.Message.Contains("parameter") || ex.Message.Contains("convert"))
                {
                    // Retry without EpochTime column
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("⚠ EpochTime column issue, inserting without it...");
                    Console.ResetColor();

                    string sqlNoEpoch = @"
INSERT INTO RawData
([DateTime],
 [DeviceID 1],[DeviceID 2],[DeviceID 3],[DeviceID 4],[DeviceID 5],
 [DeviceID 6],[DeviceID 7],[DeviceID 8],
 Flow,pH,TSS,BOD,COD,SOX,NOX,SPM)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                    using var cmd2 = new OleDbCommand(sqlNoEpoch, con);
                    cmd2.Parameters.Add("DT", OleDbType.Date).Value = measurementTime;
                    cmd2.Parameters.Add("D1", OleDbType.VarChar).Value = hasWater ? "ETP_Outlet_Flow" : (object)DBNull.Value;
                    cmd2.Parameters.Add("D2", OleDbType.VarChar).Value = hasWater ? "ETP_Outlet_pH" : (object)DBNull.Value;
                    cmd2.Parameters.Add("D3", OleDbType.VarChar).Value = hasWater ? "ETP_Outlet_TSS" : (object)DBNull.Value;
                    cmd2.Parameters.Add("D4", OleDbType.VarChar).Value = hasWater ? "ETP_Outlet_BOD" : (object)DBNull.Value;
                    cmd2.Parameters.Add("D5", OleDbType.VarChar).Value = hasWater ? "ETP_Outlet_COD" : (object)DBNull.Value;
                    cmd2.Parameters.Add("D6", OleDbType.VarChar).Value = hasAir ? "Stack_SO2" : (object)DBNull.Value;
                    cmd2.Parameters.Add("D7", OleDbType.VarChar).Value = hasAir ? "Stack_NOx" : (object)DBNull.Value;
                    cmd2.Parameters.Add("D8", OleDbType.VarChar).Value = hasAir ? "Stack_SPM" : (object)DBNull.Value;
                    cmd2.Parameters.Add("Flow", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_Flow", out var f2) ? f2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("pH", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_pH", out var ph2) ? ph2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("TSS", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_TSS", out var tss2) ? tss2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("BOD", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_BOD", out var bod2) ? bod2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("COD", OleDbType.Double).Value = values.TryGetValue("ETP_Outlet_COD", out var cod2) ? cod2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("SOX", OleDbType.Double).Value = values.TryGetValue("Stack_SO2", out var so22) ? so22 : (object)DBNull.Value;
                    cmd2.Parameters.Add("NOX", OleDbType.Double).Value = values.TryGetValue("Stack_NOx", out var nox2) ? nox2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("SPM", OleDbType.Double).Value = values.TryGetValue("Stack_SPM", out var spm2) ? spm2 : (object)DBNull.Value;
                    cmd2.ExecuteNonQuery();

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"✓ DB INSERT OK - Time: {measurementTime:yyyy-MM-dd HH:mm:ss} (EpochTime skipped)");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ DB INSERT FAILED: {ex.Message}");
                Console.ResetColor();
                throw;
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        //  InsertDistillery — mode 4 ONLY
        //  DB schema: ID | DateTime | EpochTime | DeviceID 1 | Evaportar Feed Flow
        //             | DeviceID 2 | Evaportar Concentrate Flow | DeviceID 3 | SPM
        //  NOTE: Access field names match the screenshot exactly (typo "Evaportar" included).
        // ─────────────────────────────────────────────────────────────────────
        static void InsertDistillery(Dictionary<string, double> values, long epochTime)
        {
            try
            {
                using var con = new OleDbConnection(Config.database!.connection_string);
                con.Open();

                DateTime measurementTime = epochTime > 0
                    ? DateTimeOffset.FromUnixTimeSeconds(epochTime).LocalDateTime
                    : DateTime.Now;

                bool hasFeed = values.TryGetValue("Evaporator_Feed_Flow", out var feedFlow);
                bool hasConc = values.TryGetValue("Evaporator_Concentrate_Flow", out var concFlow);
                bool hasSPM = values.TryGetValue("Boiler_Stack_SPM", out var spm);

                try
                {
                    // ── Primary INSERT with EpochTime ─────────────────────────────
                    string sql = @"
INSERT INTO RawData
([DateTime], [EpochTime],
 [DeviceID 1], [Evaportar Feed Flow],
 [DeviceID 2], [Evaportar Concentrate Flow],
 [DeviceID 3], [SPM])
VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

                    using var cmd = new OleDbCommand(sql, con);
                    cmd.Parameters.Add("DT", OleDbType.Date).Value = measurementTime;
                    cmd.Parameters.Add("Ep", OleDbType.Double).Value = (double)epochTime;
                    cmd.Parameters.Add("D1", OleDbType.VarChar).Value = hasFeed ? "Evaporator_Feed_Flow" : (object)DBNull.Value;
                    cmd.Parameters.Add("Feed", OleDbType.Double).Value = hasFeed ? feedFlow : (object)DBNull.Value;
                    cmd.Parameters.Add("D2", OleDbType.VarChar).Value = hasConc ? "Evaporator_Concentrate_Flow" : (object)DBNull.Value;
                    cmd.Parameters.Add("Conc", OleDbType.Double).Value = hasConc ? concFlow : (object)DBNull.Value;
                    cmd.Parameters.Add("D3", OleDbType.VarChar).Value = hasSPM ? "Boiler_Stack_SPM" : (object)DBNull.Value;
                    cmd.Parameters.Add("SPM", OleDbType.Double).Value = hasSPM ? spm : (object)DBNull.Value;
                    cmd.ExecuteNonQuery();

                    Console.ForegroundColor = ConsoleColor.Green;
                    string what = $"Feed={hasFeed}, Conc={hasConc}, SPM={hasSPM}";
                    Console.WriteLine($"✓ DB INSERT OK (Distillery: {what}) - Time: {measurementTime:yyyy-MM-dd HH:mm:ss}, Epoch: {epochTime}");
                    Console.ResetColor();
                }
                catch (OleDbException ex) when (ex.Message.Contains("parameter") || ex.Message.Contains("convert"))
                {
                    // ── Fallback INSERT without EpochTime ─────────────────────────
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("⚠ EpochTime column issue, inserting without it...");
                    Console.ResetColor();

                    string sqlNoEpoch = @"
INSERT INTO RawData
([DateTime],
 [DeviceID 1], [Evaportar Feed Flow],
 [DeviceID 2], [Evaportar Concentrate Flow],
 [DeviceID 3], [SPM])
VALUES (?, ?, ?, ?, ?, ?, ?)";

                    using var cmd2 = new OleDbCommand(sqlNoEpoch, con);
                    cmd2.Parameters.Add("DT", OleDbType.Date).Value = measurementTime;
                    cmd2.Parameters.Add("D1", OleDbType.VarChar).Value = hasFeed ? "Evaporator_Feed_Flow" : (object)DBNull.Value;
                    cmd2.Parameters.Add("Feed", OleDbType.Double).Value = hasFeed ? feedFlow : (object)DBNull.Value;
                    cmd2.Parameters.Add("D2", OleDbType.VarChar).Value = hasConc ? "Evaporator_Concentrate_Flow" : (object)DBNull.Value;
                    cmd2.Parameters.Add("Conc", OleDbType.Double).Value = hasConc ? concFlow : (object)DBNull.Value;
                    cmd2.Parameters.Add("D3", OleDbType.VarChar).Value = hasSPM ? "Boiler_Stack_SPM" : (object)DBNull.Value;
                    cmd2.Parameters.Add("SPM", OleDbType.Double).Value = hasSPM ? spm : (object)DBNull.Value;
                    cmd2.ExecuteNonQuery();

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"✓ DB INSERT OK (Distillery) - Time: {measurementTime:yyyy-MM-dd HH:mm:ss} (EpochTime skipped)");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ DB INSERT (Distillery) FAILED: {ex.Message}");
                Console.ResetColor();
                throw;
            }
        }

        // ═════════════════════════════════════════════════════════════════════
        //  FILE UPDATE METHODS (unchanged)
        // ═════════════════════════════════════════════════════════════════════
        static void UpdateStaFile(string filePath,
            Dictionary<string, (string nmes, string name)> mapping,
            Dictionary<string, double> values)
        {
            if (!File.Exists(filePath)) return;
            var lines = File.ReadAllLines(filePath);

            for (int i = 0; i < lines.Length; i++)
            {
                if (!lines[i].StartsWith("NMES=")) continue;
                var parts = lines[i].Split('|');
                string nmes = parts[0].Substring(5);

                foreach (var m in mapping)
                {
                    if (m.Value.nmes == nmes && values.TryGetValue(m.Key, out var v))
                    {
                        parts[3] = v.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture);
                        lines[i] = string.Join("|", parts);
                    }
                }
            }

            File.WriteAllLines(filePath, lines);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"✓ STA File Updated: {Path.GetFileName(filePath)}");
            Console.ResetColor();
        }

        static void UpdateTextFileUnit1(string filePath, Dictionary<string, double> values,
            bool hasWater, bool hasAir)
        {
            if (!File.Exists(filePath)) { Console.WriteLine($"⚠ Text file (Unit 1) not found: {filePath}"); return; }
            string content = File.ReadAllText(filePath);

            if (hasAir)
            {
                content = UpdateTextValue(content, @"PM\s+(\S+)\s+mg/Nm3", values.TryGetValue("Stack_SPM", out var spm) ? spm.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"SO2\s+(\S+)\s+mg/Nm3", values.TryGetValue("Stack_SO2", out var so2) ? so2.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"NOx\s+(\S+)\s+mg/Nm3", values.TryGetValue("Stack_NOx", out var nox) ? nox.ToString("0.00") : "NA");
            }
            if (hasWater)
            {
                content = UpdateTextValue(content, @"COD\s+(\S+)\s+mg/l", values.TryGetValue("ETP_Outlet_COD", out var cod) ? cod.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"BOD\s+(\S+)\s+mg/l", values.TryGetValue("ETP_Outlet_BOD", out var bod) ? bod.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"TSS\s+(\S+)\s+mg/l", values.TryGetValue("ETP_Outlet_TSS", out var tss) ? tss.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"pH\s+(\S+)\s+pH", values.TryGetValue("ETP_Outlet_pH", out var ph) ? ph.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"Flow\s+(\S+)\s+m3/hr", values.TryGetValue("ETP_Outlet_Flow", out var flow) ? flow.ToString("0.00") : "NA");
            }

            File.WriteAllText(filePath, content);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"✓ Text File (Unit 1) Updated: {Path.GetFileName(filePath)}");
            Console.ResetColor();
        }

        static void UpdateTextFileUnit5(string filePath, Dictionary<string, double> values,
            bool hasWater, bool hasAir)
        {
            if (!File.Exists(filePath)) { Console.WriteLine($"⚠ Text file (Unit 5) not found: {filePath}"); return; }
            string content = File.ReadAllText(filePath);

            if (hasAir)
            {
                content = UpdateTextValue(content, @"BOILER STACK :PM\s+(\S+)\s+mg/Nm3", values.TryGetValue("Stack_SPM", out var spm) ? spm.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"BOILER STACK :SO2\s+(\S+)\s+mg/Nm3", values.TryGetValue("Stack_SO2", out var so2) ? so2.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"BOILER STACK :NO\s+(\S+)\s+mg/Nm3", values.TryGetValue("Stack_NOx", out var nox) ? nox.ToString("0.00") : "NA");
            }
            if (hasWater)
            {
                content = UpdateTextValue(content, @"ETP_Outlet : COD\s+(\S+)\s+mg/l", values.TryGetValue("ETP_Outlet_COD", out var cod) ? cod.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"ETP_Outlet : BOD\s+(\S+)\s+mg/l", values.TryGetValue("ETP_Outlet_BOD", out var bod) ? bod.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"ETP_Outlet : TSS\s+(\S+)\s+mg/l", values.TryGetValue("ETP_Outlet_TSS", out var tss) ? tss.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"ETP_Outlet : PH\s+(\S+)\s+pH", values.TryGetValue("ETP_Outlet_pH", out var ph) ? ph.ToString("0.00") : "NA");
                content = UpdateTextValue(content, @"ETP_Outlet : FLOW\s+(\S+)\s+m3/hr", values.TryGetValue("ETP_Outlet_Flow", out var flow) ? flow.ToString("0.00") : "NA");
            }

            File.WriteAllText(filePath, content);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"✓ Text File (Unit 5) Updated: {Path.GetFileName(filePath)}");
            Console.ResetColor();
        }

        static void UpdateTextFileDistillery(string filePath, Dictionary<string, double> values)
        {
            if (!File.Exists(filePath)) { Console.WriteLine($"⚠ Text file (Distillery) not found: {filePath}"); return; }
            string content = File.ReadAllText(filePath);

            content = UpdateTextValue(content, @"BOILER STACK :PM\s+(\S+)\s+mg/Nm3",
                values.TryGetValue("Boiler_Stack_SPM", out var spm) ? spm.ToString("0.00") : "NA");
            content = UpdateTextValue(content, @"Flow\s+(\S+)\s+m3/hr",
                values.TryGetValue("Evaporator_Feed_Flow", out var feed) ? feed.ToString("0.00") : "NA");

            File.WriteAllText(filePath, content);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"✓ Text File (Distillery) Updated: {Path.GetFileName(filePath)}");
            Console.ResetColor();
        }

        static string UpdateTextValue(string content, string pattern, string newValue)
            => Regex.Replace(content, pattern, m => m.Value.Replace(m.Groups[1].Value, newValue));
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  STANDARD BUFFER CLASSES (modes 0-3)
    // ═════════════════════════════════════════════════════════════════════════
    class BufferedData
    {
        public long EpochTime { get; set; }
        public long WaterEpoch { get; set; } = 0;
        public long AirEpoch { get; set; } = 0;
        public long FirstArrivalEpoch { get; set; } = 0;
        public DateTime ReceivedAt { get; set; }
        public Dictionary<string, double> WaterData { get; set; } = new();
        public Dictionary<string, double> AirData { get; set; } = new();
        public bool HasWater { get; set; } = false;
        public bool HasAir { get; set; } = false;
    }

    class FlushedRecord
    {
        public long EpochTime { get; set; }
        public DateTime FlushedAt { get; set; }
        public bool HasWater { get; set; } = false;
        public bool HasAir { get; set; } = false;
        public Dictionary<string, double> WaterData { get; set; } = new();
        public Dictionary<string, double> AirData { get; set; } = new();
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  DISTILLERY BUFFER CLASSES (mode 4)
    // ═════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Live buffer: accumulates Feed Flow + Concentrate Flow + SPM until all arrive.
    /// </summary>
    class DistilleryBuffer
    {
        public long EpochTime { get; set; }
        public DateTime ReceivedAt { get; set; }
        /// <summary>All values collected so far, keyed by deviceId.</summary>
        public Dictionary<string, double> Values { get; set; } = new();
    }

    /// <summary>
    /// Tracks a distillery row that was already INSERT-ed (partially or fully),
    /// so a late-arriving payload can UPDATE it instead of creating a new row.
    /// </summary>
    class DistilleryFlushedRecord
    {
        public long EpochTime { get; set; }
        public DateTime FlushedAt { get; set; }
        public Dictionary<string, double> Values { get; set; } = new();
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  CONFIG CLASSES
    // ═════════════════════════════════════════════════════════════════════════
    class ConfigRoot
    {
        public int output_mode { get; set; } = 1;
        public MqttConfig? mqtt { get; set; }
        public FileConfig? files { get; set; }
        public DatabaseConfig? database { get; set; }
        public Dictionary<string, ParamConfig>? water_parameters { get; set; }
        public Dictionary<string, ParamConfig>? air_parameters { get; set; }
    }

    class MqttConfig
    {
        public string? broker { get; set; }
        public int port { get; set; }
        public string? topic_water { get; set; }
        public string? topic_air { get; set; }
    }

    class FileConfig
    {
        public string? water { get; set; }
        public string? air { get; set; }
        public string? display_text_unit1 { get; set; }
        public string? display_text_unit5 { get; set; }
        public string? display_text_distillery { get; set; }
    }

    class DatabaseConfig
    {
        public string? connection_string { get; set; }
    }

    class ParamConfig
    {
        public string? deviceId { get; set; }
        public string? nmes { get; set; }
    }
}
