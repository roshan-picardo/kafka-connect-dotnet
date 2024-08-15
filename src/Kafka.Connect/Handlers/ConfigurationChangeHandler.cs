using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Handlers;

public interface IConfigurationChangeHandler
{
    Task<BlockingCollection<ConnectRecord>> Refresh(IEnumerable<ConnectRecord> records, bool refresh);
}

public class ConfigurationChangeHandler(IConfigurationProvider configurationProvider) : IConfigurationChangeHandler
{
    public async Task<BlockingCollection<ConnectRecord>> Refresh(IEnumerable<ConnectRecord> records, bool refresh)
    {
        var sourceRecords = new BlockingCollection<ConnectRecord>();
        var leaderConfig = configurationProvider.GetLeaderConfig(true);
        
        var latestRecords = records.GroupBy(r => r.GetKey<string>())
            .Select(g => g.Aggregate((max, cur) => (max == null || cur.Offset > max.Offset) ? cur : max)).ToList();
       
            if (!refresh)
            {
                if (latestRecords.Count == 0)
                {
                    // local as the source of truth!!
                    foreach (var connector in leaderConfig.Connectors)
                    {
                        sourceRecords.Add(new ConfigRecord(leaderConfig.Topics.Config, connector.Key, connector.Value));
                    }
                    // no need to save the files again
                }
                else
                {
                    // kafka as source of truth!!
                    foreach (var record in latestRecords)
                    {
                        sourceRecords.Add(new ConfigRecord(leaderConfig.Topics.Config, record.GetKey<string>(), record.GetValue<JsonNode>()));
                    }
                    
                    var files = Directory.EnumerateFiles(leaderConfig.Settings, "*.json").ToList();
                    foreach (var connector in latestRecords.Where(connector =>
                                 connector.GetValue<JsonNode>() == null || connector.GetValue<JsonNode>().ToJsonString() != "{}"))
                    {
                        await WriteToFile(leaderConfig.Settings, connector.GetKey<string>(),
                            connector.GetValue<JsonNode>());
                        files.Remove(Path.Join(leaderConfig.Settings, $"{connector.GetKey<string>()}.json"));
                    }

                    foreach (var file in files)
                    {
                        File.Delete(file);
                    }
                }
            }
            else
            {
                var connectors = leaderConfig.Connectors.Keys.Union(latestRecords.Select(r => r.GetKey<string>())).Distinct();
                foreach (var connector in connectors)
                {
                    var file = leaderConfig.Connectors.SingleOrDefault(lc => lc.Key == connector);
                    sourceRecords.Add(file.Value != null
                        ? new ConfigRecord(leaderConfig.Topics.Config, connector, file.Value)
                        : new ConfigRecord(leaderConfig.Topics.Config, connector, null));
                }
                
            }
        return sourceRecords;
    }

    private static async Task WriteToFile(string folder, string key, JsonNode value)
    {
        var jsonFile = new JsonObject
        {
            {
                "leader",
                new JsonObject
                {
                    {
                        "connectors",
                        new JsonObject { { key, value } }
                    }
                }
            }
        };
        await File.WriteAllTextAsync(Path.Combine(folder, $"{key}.json"),
            jsonFile.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
    }
}
