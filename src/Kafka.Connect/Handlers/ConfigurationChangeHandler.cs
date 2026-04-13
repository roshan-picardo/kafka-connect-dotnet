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
    Task Store(IEnumerable<ConnectRecord> records, string workerName);
    ConnectRecord Configure(string connector, JsonObject settings);
}

public class ConfigurationChangeHandler(
    IConfigurationProvider configurationProvider,
    ILogger<ConfigurationChangeHandler> logger)
    : IConfigurationChangeHandler
{
    public async Task Store(IEnumerable<ConnectRecord> records, bool refresh)
    {
        using (logger.Track("Storing configurations."))
        {
            var leaderConfig = configurationProvider.GetLeaderConfig(true);

            // Ensure the Settings directory exists
            if (!Directory.Exists(leaderConfig.Settings))
            {
                Directory.CreateDirectory(leaderConfig.Settings);
                logger.Debug($"Created settings directory: {leaderConfig.Settings}");
            }

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
                    await WriteJsonToFileAsync(filePath, value);
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

    public async Task Store(IEnumerable<ConnectRecord> records, string workerName)
    {
        using (logger.Track("Storing worker configurations."))
        {
            var workerConfig = configurationProvider.GetWorkerConfig();
            var settingsPath = workerConfig?.Settings ?? Path.Combine(Directory.GetCurrentDirectory(), "worker-settings");

            if (!Directory.Exists(settingsPath))
            {
                Directory.CreateDirectory(settingsPath);
                logger.Debug($"Created worker settings directory: {settingsPath}");
            }

            var latestRecords = records
                .GroupBy(r => r.GetKey<string>())
                .Select(g => g.OrderByDescending(r => r.Offset).First())
                .ToList();

            foreach (var record in latestRecords.Select(record =>
                     {
                         record.Status = Status.Saving;
                         return record;
                     }))
            {
                var connectorKey = record.GetKey<string>();
                var value = record.GetValue<JsonNode>();

                var filePath = Path.Combine(settingsPath, $"{connectorKey}.json");

                if (value == null || value.ToJsonString() == "{}")
                {
                    record.Status = Status.Deleting;
                    if (File.Exists(filePath))
                    {
                        File.Delete(filePath);
                        logger.Debug($"Deleted configuration file: {connectorKey}.json (null/empty value)");
                    }
                    record.UpdateStatus();
                    continue;
                }

                var workersArray = value["workers"]?.AsArray();
                if (workersArray == null || workersArray.All(w => w?.GetValue<string>() != workerName))
                {
                    record.Status = Status.Deleting;
                    if (File.Exists(filePath))
                    {
                        File.Delete(filePath);
                        logger.Debug($"Deleted configuration file: {connectorKey}.json (worker '{workerName}' not in workers list)");
                    }
                    record.UpdateStatus();
                    continue;
                }

                var connectorConfig = new JsonObject();
                foreach (var property in value.AsObject())
                {
                    if (property.Key != "workers")
                    {
                        connectorConfig[property.Key] = property.Value?.DeepClone();
                    }
                }

                var workerConfigJson = new JsonObject
                {
                    {
                        "worker", new JsonObject
                        {
                            {
                                "connectors", connectorConfig
                            }
                        }
                    }
                };

                await WriteJsonToFileAsync(filePath, workerConfigJson);
                logger.Debug($"Saved configuration for connector '{connectorKey}' to {filePath}");

                record.UpdateStatus();
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

    private static async Task WriteJsonToFileAsync(string filePath, JsonNode jsonNode)
    {
        var jsonContent = jsonNode.ToJsonString(new JsonSerializerOptions
        {
            WriteIndented = true,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        });
        await using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, useAsync: true))
        await using (var writer = new StreamWriter(fileStream))
        {
            await writer.WriteAsync(jsonContent);
            await writer.FlushAsync();
            await fileStream.FlushAsync();
        }
        
        await Task.Delay(50);
    }
}
