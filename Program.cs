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
        static Dictionary<string, Dictionary<long, DateTime>> ProcessedDeviceEpochs = new();
        static Dictionary<long, DateTime> ProcessedAirEpochs = new();

        // ── STANDARD BUFFER (modes 0-3): pairs water + air ──
        static Dictionary<long, BufferedData> DataBuffer = new();
        static Dictionary<long, FlushedRecord> FlushedSoloRecords = new();

        // ── DISTILLERY BUFFER (mode 4): waits for all 3 payloads ──
        static Dictionary<long, DistilleryBuffer> DistilleryBufferMap = new();
        static Dictionary<long, DistilleryFlushedRecord> DistilleryFlushedRecords = new();

        // ── UNIT2 BUFFER (mode 5): pairs ETP water + CWF air ──
        static Dictionary<long, Unit2Buffer> Unit2BufferMap = new();
        static Dictionary<long, Unit2FlushedRecord> Unit2FlushedRecords = new();

        static System.Timers.Timer FlushTimer = new();

        static int FlushDelaySeconds = 900;
        static int EpochMatchWindowSeconds = 1800;
        static int LateArrivalWindowSeconds = 3600;

        static MqttClientOptions? MqttOptions = null;
        static IMqttClient? MqttClient = null;

        static async Task Main(string[] args)
        {
            Console.Title = "MQTT → STA/Text + Access DB (Combined Rows)";
            LoadConfig();

            // ── FIX: Mode 5 does not use WaterMapping/AirMapping (parsed by parameter name).
            //         Modes 0-4 require unique deviceId per parameter for STA file updates.
            //         Guard dictionary construction to avoid ArgumentException on duplicate keys.
            var WaterMapping = new Dictionary<string, (string nmes, string name)>();
            var AirMapping = new Dictionary<string, (string nmes, string name)>();

            if (Config.output_mode != 5)
            {
                if (Config.water_parameters != null)
                    WaterMapping = Config.water_parameters
                        .ToDictionary(k => k.Value.deviceId!, v => (v.Value.nmes!, v.Key));

                if (Config.air_parameters != null)
                    AirMapping = Config.air_parameters
                        .ToDictionary(k => k.Value.deviceId!, v => (v.Value.nmes!, v.Key));
            }

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

                    // ── MODE 5: UNIT 2 uses a different payload format ─────────────────
                    if (Config.output_mode == 5)
                    {
                        var (newValues, epochTime, stationId, deviceId) = ParsePayloadUnit2(payload);

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
                        HandleUnit2(epochTime, newValues, stationId, deviceId, isWaterTopic);
                        return;
                    }

                    // ── ALL OTHER MODES: original payload format ───────────────────────
                    var (newValuesStd, epochTimeStd) = ParsePayload(payload);

                    if (newValuesStd.Count == 0)
                    {
                        Console.WriteLine("⚠ No usable data in payload");
                        return;
                    }

                    if (epochTimeStd == 0)
                    {
                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine("⚠ No timestamp in payload, using current time");
                        Console.ResetColor();
                        epochTimeStd = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                    }

                    bool isWater = (topic == Config.mqtt!.topic_water);

                    // ── MODE 4: DISTILLERY ─────────────────────────────────────────────
                    if (Config.output_mode == 4)
                    {
                        if (isWater)
                        {
                            var distilleryWaterValues = newValuesStd
                                .Where(x => x.Key == "Evaporator_Feed_Flow" || x.Key == "Evaporator_Concentrate_Flow")
                                .ToDictionary(x => x.Key, x => x.Value);

                            if (distilleryWaterValues.Count == 0)
                            {
                                Console.WriteLine("⚠ Distillery water payload: no Evaporator_Feed_Flow or Evaporator_Concentrate_Flow found");
                                return;
                            }

                            var nonDuplicates = new Dictionary<string, double>();
                            foreach (var kv in distilleryWaterValues)
                            {
                                if (IsDeviceDuplicate(kv.Key, epochTimeStd))
                                {
                                    Console.ForegroundColor = ConsoleColor.Red;
                                    Console.WriteLine($"❌ DUPLICATE: {kv.Key} epoch={epochTimeStd} — skipping");
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
                                LastEpochTime = epochTimeStd;
                            }

                            UpdateTextFileDistillery(Config.files!.display_text_distillery!, LastValues);
                            BufferDistillery(epochTimeStd, nonDuplicates, isAir: false);
                        }
                        else
                        {
                            var distilleryAirValues = newValuesStd
                                .Where(x => x.Key == "Boiler_Stack_SPM")
                                .ToDictionary(x => x.Key, x => x.Value);

                            if (distilleryAirValues.Count == 0)
                            {
                                Console.WriteLine("⚠ Distillery air payload: no Boiler_Stack_SPM found");
                                return;
                            }

                            if (IsDeviceDuplicate("Boiler_Stack_SPM", epochTimeStd))
                            {
                                Console.ForegroundColor = ConsoleColor.Red;
                                Console.WriteLine($"❌ DUPLICATE: Boiler_Stack_SPM epoch={epochTimeStd} — skipping");
                                Console.ResetColor();
                                return;
                            }

                            lock (lockObj)
                            {
                                foreach (var kv in distilleryAirValues)
                                    LastValues[kv.Key] = kv.Value;
                                LastEpochTime = epochTimeStd;
                            }

                            UpdateTextFileDistillery(Config.files!.display_text_distillery!, LastValues);
                            BufferDistillery(epochTimeStd, distilleryAirValues, isAir: true);
                        }

                        return;
                    }

                    // ── MODES 0-3 ──────────────────────────────────────────────────────
                    if (IsDuplicate(epochTimeStd, isWater))
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"❌ DUPLICATE PAYLOAD DETECTED!");
                        Console.WriteLine($"   Topic: {topic}");
                        Console.WriteLine($"   Epoch: {epochTimeStd} ({DateTimeOffset.FromUnixTimeSeconds(epochTimeStd).LocalDateTime:yyyy-MM-dd HH:mm:ss})");
                        Console.WriteLine($"   → SKIPPING to prevent duplicate DB insert");
                        Console.ResetColor();
                        return;
                    }

                    lock (lockObj)
                    {
                        foreach (var kv in newValuesStd)
                            LastValues[kv.Key] = kv.Value;
                        LastEpochTime = epochTimeStd;
                    }

                    if (isWater)
                    {
                        var waterValues = newValuesStd
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

                        BufferData(epochTimeStd, waterValues, isWater: true);
                    }
                    else if (topic == Config.mqtt!.topic_air)
                    {
                        Console.WriteLine($"DEBUG: Received {newValuesStd.Count} total values");
                        foreach (var kv in newValuesStd)
                            Console.WriteLine($"  DeviceID: {kv.Key}");

                        if (Config.output_mode == 3)
                        {
                            var cogenerationValues = newValuesStd
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
                            BufferData(epochTimeStd, normalizedValues, isWater: false);
                            return;
                        }

                        var normalizedAirValues = new Dictionary<string, double>();
                        foreach (var kv in newValuesStd)
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

                        BufferData(epochTimeStd, normalizedAirValues, isWater: false);
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

            MqttOptions = new MqttClientOptionsBuilder()
                .WithTcpServer(Config.mqtt!.broker, Config.mqtt.port)
                .WithClientId("STA-DB-" + Environment.MachineName)
                .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
                .WithCleanSession(false)
                .Build();

            MqttClient = mqttClient;

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
                5 => "Unit 2 — ETP Outlet + CWF/CSW DB",
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
            if (Config.output_mode == 5)
                Console.WriteLine("✓ Unit 2 mode: pairing ETP water data with CWF/CSW air data per epoch");
            Console.WriteLine("✓ Waiting for data...\n");
            Console.ResetColor();

            await Task.Delay(-1);
        }

        // ═════════════════════════════════════════════════════════════════════
        //  MODE 5 — UNIT 2
        //  Payload format: { "data": [ { "stationId": "...", "device_data": [ {
        //    "deviceId": "...", "params": [ { "parameter": "...", "value": ...,
        //    "timestamp": ... } ] } ] } ] }
        //
        //  Water topic  → StationID 1 / DeviceID 1 → Flow, pH, TSS, BOD, COD
        //  Air topic    → StationID 2 / DeviceID 2 → Flow(CWF), MassFlow(CSW)
        // ═════════════════════════════════════════════════════════════════════

        static (Dictionary<string, double> values, long epochTime, string stationId, string deviceId)
            ParsePayloadUnit2(string raw)
        {
            var result = new Dictionary<string, double>();
            long epochTime = 0;
            string stationId = "";
            string deviceId = "";

            if (string.IsNullOrWhiteSpace(raw)) return (result, epochTime, stationId, deviceId);

            try
            {
                using var doc = JsonDocument.Parse(raw);
                var root = doc.RootElement;

                // Support both { "data": [...] } and bare array
                JsonElement dataArray;
                if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("data", out var dataProp))
                    dataArray = dataProp;
                else if (root.ValueKind == JsonValueKind.Array)
                    dataArray = root;
                else
                {
                    Console.WriteLine("⚠ Unit 2 payload: unexpected JSON structure");
                    return (result, epochTime, stationId, deviceId);
                }

                foreach (var station in dataArray.EnumerateArray())
                {
                    if (station.TryGetProperty("stationId", out var stationEl))
                        stationId = stationEl.GetString() ?? "";

                    if (!station.TryGetProperty("device_data", out var deviceData)) continue;

                    foreach (var device in deviceData.EnumerateArray())
                    {
                        if (device.TryGetProperty("deviceId", out var devEl))
                            deviceId = devEl.GetString() ?? "";

                        if (!device.TryGetProperty("params", out var paramsList)) continue;

                        foreach (var param in paramsList.EnumerateArray())
                        {
                            // Extract timestamp (milliseconds → seconds)
                            if (epochTime == 0 && param.TryGetProperty("timestamp", out var tsEl))
                                epochTime = ExtractEpochTime(tsEl);

                            if (!param.TryGetProperty("parameter", out var paramNameEl)) continue;
                            if (!param.TryGetProperty("value", out var valueEl)) continue;

                            string paramName = paramNameEl.GetString() ?? "";

                            double val;
                            if (valueEl.ValueKind == JsonValueKind.Number)
                                val = valueEl.GetDouble();
                            else if (valueEl.ValueKind == JsonValueKind.String)
                            {
                                if (!double.TryParse(valueEl.GetString(),
                                    System.Globalization.NumberStyles.Any,
                                    System.Globalization.CultureInfo.InvariantCulture, out val))
                                    continue;
                            }
                            else continue;

                            // Map parameter names → internal keys
                            string key = paramName.ToLowerInvariant() switch
                            {
                                "cod" => "Unit2_COD",
                                "bod" => "Unit2_BOD",
                                "ph" => "Unit2_pH",
                                "tss" => "Unit2_TSS",
                                "flow" => "Unit2_Flow",        // ETP outlet flow (water topic)
                                "mass_flow" => "Unit2_MassFlow",    // CSW (air topic)
                                _ => ""
                            };

                            if (string.IsNullOrEmpty(key))
                            {
                                Console.WriteLine($"  ⚠ Unknown parameter '{paramName}' — skipped");
                                continue;
                            }

                            result[key] = val;
                            Console.WriteLine($"  {paramName} ({key}) = {val}");
                        }
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
                Console.WriteLine("Unit2 parse error: " + ex.Message);
            }

            return (result, epochTime, stationId, deviceId);
        }

        static void HandleUnit2(long epochTime, Dictionary<string, double> values,
            string stationId, string deviceId, bool isWaterTopic)
        {
            // For the air topic, "Unit2_Flow" is actually CWF (not ETP flow).
            // Rename it so it doesn't collide with the water-side ETP flow.
            if (!isWaterTopic && values.ContainsKey("Unit2_Flow"))
            {
                values["Unit2_FlowCWF"] = values["Unit2_Flow"];
                values.Remove("Unit2_Flow");
            }

            // Deduplicate per side
            string dedupKey = isWaterTopic ? "__unit2_water__" : "__unit2_air__";
            if (IsDeviceDuplicate(dedupKey, epochTime))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ DUPLICATE Unit2 {(isWaterTopic ? "WATER" : "AIR")} epoch={epochTime} — skipping");
                Console.ResetColor();
                return;
            }

            lock (lockObj)
            {
                foreach (var kv in values)
                    LastValues[kv.Key] = kv.Value;
                LastEpochTime = epochTime;
            }

            // Optional: update display text file for Unit 2
            if (!string.IsNullOrEmpty(Config.files?.display_text_unit2))
                UpdateTextFileUnit2(Config.files.display_text_unit2, LastValues);

            BufferUnit2(epochTime, values, stationId, deviceId, isWaterTopic);
        }

        // ─────────────────────────────────────────────────────────────────────
        //  BufferUnit2 — waits for water + air before inserting
        // ─────────────────────────────────────────────────────────────────────
        static void BufferUnit2(long epochTime, Dictionary<string, double> values,
            string stationId, string deviceId, bool isWaterTopic)
        {
            lock (lockObj)
            {
                string side = isWaterTopic ? "WATER" : "AIR";

                // PATH 1: Exact-epoch match in active buffer
                if (Unit2BufferMap.TryGetValue(epochTime, out var existing))
                {
                    bool waitingForThisSide = isWaterTopic ? !existing.HasWater : !existing.HasAir;
                    if (waitingForThisSide)
                    {
                        MergeUnit2Buffer(existing, values, stationId, deviceId, isWaterTopic);
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine($"  🔗 Unit2 {side} matched existing buffer (exact epoch={epochTime})");
                        Console.ResetColor();

                        double waited = (DateTime.Now - existing.ReceivedAt).TotalSeconds;
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"  ✓ COMPLETE: Water+Air paired. Waited {waited:0.0}s.");
                        Console.ResetColor();

                        FlushUnit2Buffer(epochTime, existing);
                        Unit2BufferMap.Remove(epochTime);
                        return;
                    }
                }

                // PATH 2: Fuzzy match within EpochMatchWindowSeconds
                Unit2Buffer? fuzzyMatch = null;
                long fuzzyKey = 0;
                foreach (var kv in Unit2BufferMap)
                {
                    long diff = Math.Abs(kv.Key - epochTime);
                    bool waitingForThisSide = isWaterTopic ? !kv.Value.HasWater : !kv.Value.HasAir;
                    if (diff > 0 && diff <= EpochMatchWindowSeconds && waitingForThisSide)
                    {
                        if (fuzzyMatch == null || diff < Math.Abs(fuzzyKey - epochTime))
                        {
                            fuzzyMatch = kv.Value;
                            fuzzyKey = kv.Key;
                        }
                    }
                }

                if (fuzzyMatch != null)
                {
                    long diff = Math.Abs(fuzzyKey - epochTime);
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"  🔗 Unit2 {side}: fuzzy match (diff={diff}s) merged into buffer (bufferEpoch={fuzzyKey})");
                    Console.ResetColor();
                    MergeUnit2Buffer(fuzzyMatch, values, stationId, deviceId, isWaterTopic);

                    if (fuzzyMatch.HasWater && fuzzyMatch.HasAir)
                    {
                        FlushUnit2Buffer(fuzzyKey, fuzzyMatch);
                        Unit2BufferMap.Remove(fuzzyKey);
                    }
                    return;
                }

                // PATH 3: Late arrival — already flushed solo
                Unit2FlushedRecord? flushedMatch = null;
                long flushedKey = 0;
                foreach (var kv in Unit2FlushedRecords)
                {
                    long diff = Math.Abs(kv.Key - epochTime);
                    bool missingThisSide = isWaterTopic ? !kv.Value.HasWater : !kv.Value.HasAir;
                    if (diff <= EpochMatchWindowSeconds && missingThisSide)
                    {
                        if (flushedMatch == null || diff < Math.Abs(flushedKey - epochTime))
                        {
                            flushedMatch = kv.Value;
                            flushedKey = kv.Key;
                        }
                    }
                }

                if (flushedMatch != null)
                {
                    long diff = Math.Abs(flushedKey - epochTime);
                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine($"\n  ⚡ LATE ARRIVAL (Unit2): {side} (epoch={epochTime}) matched flushed record (epoch={flushedKey}, diff={diff}s) → UPDATE");
                    Console.ResetColor();

                    foreach (var kv in values) flushedMatch.Values[kv.Key] = kv.Value;
                    if (isWaterTopic)
                    {
                        flushedMatch.StationID1 = stationId;
                        flushedMatch.DeviceID1 = deviceId;
                        flushedMatch.HasWater = true;
                    }
                    else
                    {
                        flushedMatch.StationID2 = stationId;
                        flushedMatch.DeviceID2 = deviceId;
                        flushedMatch.HasAir = true;
                    }

                    UpdateUnit2Row(flushedMatch.Values, flushedKey,
                        flushedMatch.StationID1, flushedMatch.DeviceID1,
                        flushedMatch.StationID2, flushedMatch.DeviceID2,
                        flushedMatch.HasWater, flushedMatch.HasAir);
                    Unit2FlushedRecords[flushedKey] = flushedMatch;
                    return;
                }

                // PATH 4: New buffer entry
                var newBuf = new Unit2Buffer
                {
                    EpochTime = epochTime,
                    ReceivedAt = DateTime.Now
                };
                MergeUnit2Buffer(newBuf, values, stationId, deviceId, isWaterTopic);
                Unit2BufferMap[epochTime] = newBuf;

                string arrivedKeys = string.Join(", ", values.Keys);
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"  🔵 Unit2 {side} buffered [{arrivedKeys}] (epoch={epochTime}) — waiting {FlushDelaySeconds}s for partner...");
                Console.ResetColor();
            }
        }

        static void MergeUnit2Buffer(Unit2Buffer buf, Dictionary<string, double> values,
            string stationId, string deviceId, bool isWaterTopic)
        {
            foreach (var kv in values) buf.Values[kv.Key] = kv.Value;
            if (isWaterTopic)
            {
                buf.HasWater = true;
                buf.StationID1 = stationId;
                buf.DeviceID1 = deviceId;
            }
            else
            {
                buf.HasAir = true;
                buf.StationID2 = stationId;
                buf.DeviceID2 = deviceId;
            }
        }

        static void FlushUnit2Buffer(long epochTime, Unit2Buffer buf)
        {
            // Must be called inside lockObj
            InsertUnit2(buf.Values, epochTime,
                buf.StationID1, buf.DeviceID1,
                buf.StationID2, buf.DeviceID2,
                buf.HasWater, buf.HasAir);

            if (!(buf.HasWater && buf.HasAir))
            {
                var flushed = new Unit2FlushedRecord
                {
                    EpochTime = epochTime,
                    FlushedAt = DateTime.Now,
                    HasWater = buf.HasWater,
                    HasAir = buf.HasAir,
                    StationID1 = buf.StationID1,
                    DeviceID1 = buf.DeviceID1,
                    StationID2 = buf.StationID2,
                    DeviceID2 = buf.DeviceID2
                };
                foreach (var kv in buf.Values) flushed.Values[kv.Key] = kv.Value;
                Unit2FlushedRecords[epochTime] = flushed;

                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"  📋 Unit2 solo record registered for late-arrival UPDATE (epoch={epochTime}, window={LateArrivalWindowSeconds}s)");
                Console.ResetColor();
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        //  InsertUnit2
        //  Schema: ID | DateTime | EpochTime | StationID 1 | DeviceID 1 |
        //          Flow | pH | TSS | BOD | COD |
        //          StationID 2 | DeviceID 2 | Flow(CWF) | MassFlow(CSW)
        // ─────────────────────────────────────────────────────────────────────
        static void InsertUnit2(Dictionary<string, double> values, long epochTime,
            string stationId1, string deviceId1,
            string stationId2, string deviceId2,
            bool hasWater, bool hasAir)
        {
            try
            {
                // ── Use connection_string_unit2 if provided, else fall back to connection_string
                string connStr = !string.IsNullOrEmpty(Config.database?.connection_string_unit2)
                    ? Config.database.connection_string_unit2
                    : Config.database!.connection_string!;

                using var con = new OleDbConnection(connStr);
                con.Open();

                DateTime measurementTime = epochTime > 0
                    ? DateTimeOffset.FromUnixTimeSeconds(epochTime).LocalDateTime
                    : DateTime.Now;

                try
                {
                    string sql = @"
INSERT INTO RawData
([DateTime], [EpochTime],
 [StationID 1], [DeviceID 1], [Flow], [pH], [TSS], [BOD], [COD],
 [StationID 2], [DeviceID 2], [Flow(CWF)], [MassFlow(CSW)])
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    using var cmd = new OleDbCommand(sql, con);
                    cmd.Parameters.Add("DT", OleDbType.Date).Value = measurementTime;
                    cmd.Parameters.Add("Ep", OleDbType.Double).Value = (double)epochTime;

                    cmd.Parameters.Add("S1", OleDbType.VarChar).Value = hasWater ? stationId1 : (object)DBNull.Value;
                    cmd.Parameters.Add("D1", OleDbType.VarChar).Value = hasWater ? deviceId1 : (object)DBNull.Value;
                    cmd.Parameters.Add("Fl", OleDbType.Double).Value = values.TryGetValue("Unit2_Flow", out var fl) ? fl : (object)DBNull.Value;
                    cmd.Parameters.Add("PH", OleDbType.Double).Value = values.TryGetValue("Unit2_pH", out var ph) ? ph : (object)DBNull.Value;
                    cmd.Parameters.Add("TS", OleDbType.Double).Value = values.TryGetValue("Unit2_TSS", out var tss) ? tss : (object)DBNull.Value;
                    cmd.Parameters.Add("BD", OleDbType.Double).Value = values.TryGetValue("Unit2_BOD", out var bod) ? bod : (object)DBNull.Value;
                    cmd.Parameters.Add("CD", OleDbType.Double).Value = values.TryGetValue("Unit2_COD", out var cod) ? cod : (object)DBNull.Value;

                    cmd.Parameters.Add("S2", OleDbType.VarChar).Value = hasAir ? stationId2 : (object)DBNull.Value;
                    cmd.Parameters.Add("D2", OleDbType.VarChar).Value = hasAir ? deviceId2 : (object)DBNull.Value;
                    cmd.Parameters.Add("CWF", OleDbType.Double).Value = values.TryGetValue("Unit2_FlowCWF", out var cwf) ? cwf : (object)DBNull.Value;
                    cmd.Parameters.Add("CSW", OleDbType.Double).Value = values.TryGetValue("Unit2_MassFlow", out var csw) ? csw : (object)DBNull.Value;

                    cmd.ExecuteNonQuery();

                    string dataType = (hasWater && hasAir) ? "COMBINED" : hasWater ? "Water Only" : "Air Only";
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"✓ DB INSERT OK (Unit2 {dataType}) - Time: {measurementTime:yyyy-MM-dd HH:mm:ss}, Epoch: {epochTime}");
                    Console.ResetColor();
                }
                catch (OleDbException ex) when (ex.Message.Contains("parameter") || ex.Message.Contains("convert"))
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("⚠ EpochTime column issue, inserting without it...");
                    Console.ResetColor();

                    string sqlNoEpoch = @"
INSERT INTO RawData
([DateTime],
 [StationID 1], [DeviceID 1], [Flow], [pH], [TSS], [BOD], [COD],
 [StationID 2], [DeviceID 2], [Flow(CWF)], [MassFlow(CSW)])
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    using var cmd2 = new OleDbCommand(sqlNoEpoch, con);
                    cmd2.Parameters.Add("DT", OleDbType.Date).Value = measurementTime;
                    cmd2.Parameters.Add("S1", OleDbType.VarChar).Value = hasWater ? stationId1 : (object)DBNull.Value;
                    cmd2.Parameters.Add("D1", OleDbType.VarChar).Value = hasWater ? deviceId1 : (object)DBNull.Value;
                    cmd2.Parameters.Add("Fl", OleDbType.Double).Value = values.TryGetValue("Unit2_Flow", out var fl) ? fl : (object)DBNull.Value;
                    cmd2.Parameters.Add("PH", OleDbType.Double).Value = values.TryGetValue("Unit2_pH", out var ph) ? ph : (object)DBNull.Value;
                    cmd2.Parameters.Add("TS", OleDbType.Double).Value = values.TryGetValue("Unit2_TSS", out var tss) ? tss : (object)DBNull.Value;
                    cmd2.Parameters.Add("BD", OleDbType.Double).Value = values.TryGetValue("Unit2_BOD", out var bod) ? bod : (object)DBNull.Value;
                    cmd2.Parameters.Add("CD", OleDbType.Double).Value = values.TryGetValue("Unit2_COD", out var cod) ? cod : (object)DBNull.Value;
                    cmd2.Parameters.Add("S2", OleDbType.VarChar).Value = hasAir ? stationId2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("D2", OleDbType.VarChar).Value = hasAir ? deviceId2 : (object)DBNull.Value;
                    cmd2.Parameters.Add("CWF", OleDbType.Double).Value = values.TryGetValue("Unit2_FlowCWF", out var cwf) ? cwf : (object)DBNull.Value;
                    cmd2.Parameters.Add("CSW", OleDbType.Double).Value = values.TryGetValue("Unit2_MassFlow", out var csw) ? csw : (object)DBNull.Value;
                    cmd2.ExecuteNonQuery();

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"✓ DB INSERT OK (Unit2) - Time: {measurementTime:yyyy-MM-dd HH:mm:ss} (EpochTime skipped)");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ DB INSERT (Unit2) FAILED: {ex.Message}");
                Console.ResetColor();
                throw;
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        //  UpdateUnit2Row — UPDATE an already-inserted Unit 2 row
        // ─────────────────────────────────────────────────────────────────────
        static void UpdateUnit2Row(Dictionary<string, double> values, long epochTime,
            string stationId1, string deviceId1,
            string stationId2, string deviceId2,
            bool hasWater, bool hasAir)
        {
            try
            {
                string connStr = !string.IsNullOrEmpty(Config.database?.connection_string_unit2)
                    ? Config.database.connection_string_unit2
                    : Config.database!.connection_string!;

                using var con = new OleDbConnection(connStr);
                con.Open();

                DateTime measurementTime = epochTime > 0
                    ? DateTimeOffset.FromUnixTimeSeconds(epochTime).LocalDateTime
                    : DateTime.Now;

                var setClauses = new List<string>();
                var parameters = new List<(string name, OleDbType type, object val)>();

                if (hasWater)
                {
                    setClauses.Add("[StationID 1]=?, [DeviceID 1]=?, [Flow]=?, [pH]=?, [TSS]=?, [BOD]=?, [COD]=?");
                    parameters.Add(("S1", OleDbType.VarChar, (object)stationId1));
                    parameters.Add(("D1", OleDbType.VarChar, (object)deviceId1));
                    parameters.Add(("Fl", OleDbType.Double, values.TryGetValue("Unit2_Flow", out var fl) ? fl : DBNull.Value));
                    parameters.Add(("PH", OleDbType.Double, values.TryGetValue("Unit2_pH", out var ph) ? ph : DBNull.Value));
                    parameters.Add(("TS", OleDbType.Double, values.TryGetValue("Unit2_TSS", out var tss) ? tss : DBNull.Value));
                    parameters.Add(("BD", OleDbType.Double, values.TryGetValue("Unit2_BOD", out var bod) ? bod : DBNull.Value));
                    parameters.Add(("CD", OleDbType.Double, values.TryGetValue("Unit2_COD", out var cod) ? cod : DBNull.Value));
                }

                if (hasAir)
                {
                    setClauses.Add("[StationID 2]=?, [DeviceID 2]=?, [Flow(CWF)]=?, [MassFlow(CSW)]=?");
                    parameters.Add(("S2", OleDbType.VarChar, (object)stationId2));
                    parameters.Add(("D2", OleDbType.VarChar, (object)deviceId2));
                    parameters.Add(("CWF", OleDbType.Double, values.TryGetValue("Unit2_FlowCWF", out var cwf) ? cwf : DBNull.Value));
                    parameters.Add(("CSW", OleDbType.Double, values.TryGetValue("Unit2_MassFlow", out var csw) ? csw : DBNull.Value));
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
                    cmd.Parameters.Add("Ep", OleDbType.Double).Value = (double)epochTime;

                    int rows = cmd.ExecuteNonQuery();
                    LogUpdateResult(rows, epochTime, measurementTime, "Unit2 (late)");
                    if (rows > 0) return;
                }
                catch (OleDbException) { /* fall through to DateTime fallback */ }

                string sqlDt = $"UPDATE RawData SET {string.Join(", ", setClauses)} WHERE [DateTime] = ?";
                using var cmd2 = new OleDbCommand(sqlDt, con);
                foreach (var (name, type, val) in parameters)
                    cmd2.Parameters.Add(name, type).Value = val;
                cmd2.Parameters.Add("DT", OleDbType.Date).Value = measurementTime;

                int rows2 = cmd2.ExecuteNonQuery();
                LogUpdateResult(rows2, epochTime, measurementTime, "Unit2 (late, DateTime fallback)");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ DB UPDATE (Unit2) FAILED: {ex.Message}");
                Console.ResetColor();
            }
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

            if (Config.output_mode < 0 || Config.output_mode > 5)
            {
                Console.WriteLine("Invalid output_mode in config. Must be 0-5.");
                Environment.Exit(1);
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        //  ParsePayload — original format (modes 0-4)
        // ─────────────────────────────────────────────────────────────────────
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
        // ═════════════════════════════════════════════════════════════════════
        static void BufferDistillery(long epochTime, Dictionary<string, double> values, bool isAir)
        {
            lock (lockObj)
            {
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

                DistilleryBuffer? fuzzyMatch = null;
                long fuzzyKey = 0;

                foreach (var kv in DistilleryBufferMap)
                {
                    long diff = Math.Abs(kv.Key - epochTime);
                    if (diff > 0 && diff <= EpochMatchWindowSeconds)
                    {
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

        static void TryFlushDistillery(long epochKey, DistilleryBuffer buf)
        {
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
        }

        static void UpdateDistilleryRow(Dictionary<string, double> values, long epochTime)
        {
            try
            {
                using var con = new OleDbConnection(Config.database!.connection_string);
                con.Open();

                DateTime measurementTime = epochTime > 0
                    ? DateTimeOffset.FromUnixTimeSeconds(epochTime).LocalDateTime
                    : DateTime.Now;

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
                catch (OleDbException) { }

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
        //  FlushOldBufferedData — timer fires every 60s
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

                    var distilleryToExpire = DistilleryFlushedRecords
                        .Where(kv => (now - kv.Value.FlushedAt).TotalSeconds >= LateArrivalWindowSeconds)
                        .Select(kv => kv.Key).ToList();
                    foreach (var key in distilleryToExpire)
                    {
                        Console.WriteLine($"  🗑 Distillery: Expired late-arrival window for epoch={key}");
                        DistilleryFlushedRecords.Remove(key);
                    }

                    // ── Flush timed-out Unit 2 buffer entries (mode 5) ───────────────────
                    var unit2ToFlush = Unit2BufferMap
                        .Where(kv => (now - kv.Value.ReceivedAt).TotalSeconds >= FlushDelaySeconds)
                        .Select(kv => kv.Key).ToList();

                    foreach (var epochTime in unit2ToFlush)
                    {
                        var buf = Unit2BufferMap[epochTime];
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"\n⚠ UNIT2 TIMEOUT ({FlushDelaySeconds}s): epoch={epochTime}");
                        Console.WriteLine($"  HasWater={buf.HasWater}, HasAir={buf.HasAir}");
                        Console.WriteLine($"  Inserting partial row. Late arrival can UPDATE for {LateArrivalWindowSeconds}s.");
                        Console.ResetColor();

                        FlushUnit2Buffer(epochTime, buf);
                        Unit2BufferMap.Remove(epochTime);
                    }

                    var unit2ToExpire = Unit2FlushedRecords
                        .Where(kv => (now - kv.Value.FlushedAt).TotalSeconds >= LateArrivalWindowSeconds)
                        .Select(kv => kv.Key).ToList();
                    foreach (var key in unit2ToExpire)
                    {
                        Console.WriteLine($"  🗑 Unit2: Expired late-arrival window for epoch={key}");
                        Unit2FlushedRecords.Remove(key);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ ERROR in flush timer: {ex.Message}");
                Console.ResetColor();
            }
        }

        // ═════════════════════════════════════════════════════════════════════
        //  MODES 0-3 BUFFER LOGIC
        // ═════════════════════════════════════════════════════════════════════
        static void BufferData(long epochTime, Dictionary<string, double> values, bool isWater)
        {
            lock (lockObj)
            {
                string side = isWater ? "WATER" : "AIR";

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
        //  InsertCombined — modes 0-3
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
        //  FILE UPDATE METHODS
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

        static void UpdateTextFileUnit2(string filePath, Dictionary<string, double> values)
        {
            if (!File.Exists(filePath)) { Console.WriteLine($"⚠ Text file (Unit 2) not found: {filePath}"); return; }
            string content = File.ReadAllText(filePath);

            content = UpdateTextValue(content, @"COD\s+(\S+)\s+mg/l", values.TryGetValue("Unit2_COD", out var cod) ? cod.ToString("0.00") : "NA");
            content = UpdateTextValue(content, @"BOD\s+(\S+)\s+mg/l", values.TryGetValue("Unit2_BOD", out var bod) ? bod.ToString("0.00") : "NA");
            content = UpdateTextValue(content, @"TSS\s+(\S+)\s+mg/l", values.TryGetValue("Unit2_TSS", out var tss) ? tss.ToString("0.00") : "NA");
            content = UpdateTextValue(content, @"pH\s+(\S+)\s+pH", values.TryGetValue("Unit2_pH", out var ph) ? ph.ToString("0.00") : "NA");
            content = UpdateTextValue(content, @"ETP Flow\s+(\S+)\s+m3/hr", values.TryGetValue("Unit2_Flow", out var fl) ? fl.ToString("0.00") : "NA");
            content = UpdateTextValue(content, @"CWF Flow\s+(\S+)\s+m3/hr", values.TryGetValue("Unit2_FlowCWF", out var cwf) ? cwf.ToString("0.00") : "NA");
            content = UpdateTextValue(content, @"MassFlow\s+(\S+)\s+Kg/Hr", values.TryGetValue("Unit2_MassFlow", out var mf) ? mf.ToString("0.00") : "NA");

            File.WriteAllText(filePath, content);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"✓ Text File (Unit 2) Updated: {Path.GetFileName(filePath)}");
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
    class DistilleryBuffer
    {
        public long EpochTime { get; set; }
        public DateTime ReceivedAt { get; set; }
        public Dictionary<string, double> Values { get; set; } = new();
    }

    class DistilleryFlushedRecord
    {
        public long EpochTime { get; set; }
        public DateTime FlushedAt { get; set; }
        public Dictionary<string, double> Values { get; set; } = new();
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  UNIT 2 BUFFER CLASSES (mode 5)
    // ═════════════════════════════════════════════════════════════════════════
    class Unit2Buffer
    {
        public long EpochTime { get; set; }
        public DateTime ReceivedAt { get; set; }
        public bool HasWater { get; set; } = false;
        public bool HasAir { get; set; } = false;
        public string StationID1 { get; set; } = "";
        public string DeviceID1 { get; set; } = "";
        public string StationID2 { get; set; } = "";
        public string DeviceID2 { get; set; } = "";
        public Dictionary<string, double> Values { get; set; } = new();
    }

    class Unit2FlushedRecord
    {
        public long EpochTime { get; set; }
        public DateTime FlushedAt { get; set; }
        public bool HasWater { get; set; } = false;
        public bool HasAir { get; set; } = false;
        public string StationID1 { get; set; } = "";
        public string DeviceID1 { get; set; } = "";
        public string StationID2 { get; set; } = "";
        public string DeviceID2 { get; set; } = "";
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
        public string? display_text_unit2 { get; set; }
    }

    class DatabaseConfig
    {
        public string? connection_string { get; set; }
        public string? connection_string_unit2 { get; set; }
    }

    class ParamConfig
    {
        public string? deviceId { get; set; }
        public string? nmes { get; set; }
    }
}