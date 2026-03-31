using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Handlers;

public interface IConfigurationChangeHandler
{
    Task Store(IEnumerable<ConnectRecord> records, bool refresh);
    ConnectRecord Configure(string connector, JsonObject settings);
}

public class ConfigurationChangeHandler(IConfigurationProvider configurationProvider, ILogger<ConfigurationChangeHandler> logger) : IConfigurationChangeHandler
{
    public async Task Store(IEnumerable<ConnectRecord> records, bool refresh)
    {
        using (logger.Track("Storing configurations."))
        {
            var leaderConfig = configurationProvider.GetLeaderConfig(true);

            var existingFiles = Directory.Exists(leaderConfig.Settings)
                ? Directory.EnumerateFiles(leaderConfig.Settings, "*.json").ToList()
                : [];

            foreach (var record in records.Select(record =>
                     {
                         record.Status = Status.Saving;
                         return record;
                     }))
            {
                var connector = record.GetKey<string>();
                var value = record.GetValue<JsonNode>();

                var filePath = Path.Combine(leaderConfig.Settings, $"{connector}.json");

                if (value != null && value.ToJsonString() != "{}")
                {

                    await File.WriteAllTextAsync(Path.Combine(leaderConfig.Settings, $"{connector}.json"),
                        value.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
                    existingFiles.Remove(filePath);
                }
                else
                {
                    record.Status = Status.Deleting;
                    File.Delete(filePath);
                }

                record.UpdateStatus();
            }

            if (refresh)
            {
                foreach (var file in existingFiles)
                {
                    File.Delete(file);
                }
            }
        }
    }

    public ConnectRecord Configure(string connector, JsonObject settings)
    {
        var leaderConfig = configurationProvider.GetLeaderConfig();
        if (settings != null && settings.ContainsKey("connector"))
        {
            var connectorValue = settings["connector"];
            settings.Remove("connector");
            settings[connector] = connectorValue;
        }

        return new ConnectRecord(leaderConfig.GetConfigurationTopic(), -1, -1)
        {
            Status = Status.Selected,
            Deserialized = new ConnectMessage<JsonNode>()
            {
                Key = connector,
                Value = settings
            }
        };
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
