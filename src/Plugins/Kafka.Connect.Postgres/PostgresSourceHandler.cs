using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Postgres.Models;
using Npgsql;

namespace Kafka.Connect.Postgres;

public class PostgresSourceHandler(
    IConfigurationProvider configurationProvider,
    IReadWriteStrategyProvider readWriteStrategyProvider,
    IPostgresClientProvider postgresClientProvider,
    ILogger<PostgresSourceHandler> logger)
    : SourceHandler(configurationProvider, readWriteStrategyProvider)
{
    private readonly IConfigurationProvider _configurationProvider = configurationProvider;

    public override async Task<IList<ConnectRecord>> Get(string connector, int taskId, CommandRecord command)
    {
        using (logger.Track("Getting batch of records"))
        {
            var changeLog = _configurationProvider.GetPluginConfig<SourceConfig>(connector).Changelog;
            command.Changelog = JsonSerializer.SerializeToNode(changeLog);
            var model = await GetReadWriteStrategy(connector, command).Build<string>(connector, command);
            
            await using var reader = await new NpgsqlCommand(model.Model,
                    postgresClientProvider.GetPostgresClient(connector, taskId).GetConnection())
                .ExecuteReaderAsync();
            var records = new List<ConnectRecord>();
            while (await reader.ReadAsync())
            {
                var record = new Dictionary<string, object>();
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    record.Add(reader.GetName(i), reader.GetValue(i));
                }

                records.Add(GetConnectMessage(record, command));
            }
        
            await reader.CloseAsync();

            return records;
        }
    }

    public override IDictionary<string, Command> GetCommands(string connector)
    {
        var config = _configurationProvider.GetPluginConfig<SourceConfig>(connector);
        return config.Commands.ToDictionary(k => k.Key, v => v.Value as Command);
    }

    public override CommandRecord GetUpdatedCommand(CommandRecord command, IList<ConnectMessage<JsonNode>> records)
    {
        var config = command.Get<CommandConfig>();
        if (!command.IsChangeLog())
        {
            if (records.Count > 0)
            {
                var sorted = records.Select(r=> r.Value["new"].ToDictionary("new", true)).OrderBy(_ => 1);
                foreach (var key in config.Filters.Keys)
                {
                    sorted = sorted.ThenBy(d => d[key]);
                }

                config.Filters.Values = sorted.LastOrDefault()?.Where(x => config.Filters.Keys.Contains(x.Key)).ToDictionary();
            }
        }
        else if(config.IsSnapshot())
        {
            if (config.IsInitial())
            {
                var value = records.Single().Convert().Value;
                if(value.TryGetValue("_total", out var total))
                {
                    config.Snapshot.Total = Convert.ToInt64(total);
                }
                if(value.TryGetValue("_timestamp", out var timestamp))
                {
                    config.Snapshot.Timestamp = Convert.ToInt64(timestamp);
                }
            }
            else
            {
                if(records.Count > 0)
                {
                    config.Snapshot.Id = records.Max(m => m.Value["_row"]!.GetValue<long>());
                }

                if (config.Snapshot.Total >= config.Snapshot.Id)
                {
                    config.Snapshot.Enabled = false;
                    config.Snapshot.Id = 0;
                    config.Snapshot.Total = -1;
                }
            }
        }
        else
        {
            if (records.Count > 0)
            {
                config.Snapshot.Timestamp = records.Max(m => m.Value["timestamp"]!.GetValue<long>());
                config.Snapshot.Id = records.Max(m => m.Value["id"]!.GetValue<long>());
            }
        }

        command.Command = config.ToJson();
        return command;
    }

    private static ConnectRecord GetConnectMessage(Dictionary<string, object> message, CommandRecord command)
    {
        var config = command.Get<CommandConfig>();
        JsonNode value;
        
        if (!command.IsChangeLog())
        {
            value = new JsonObject
            {
                { "operation", "CHANGE" },
                { "timestamp", DateTime.UtcNow.ToUnixMicroseconds() },
                { "old", null },
                { "new", message.ToJson() }
            };
        }
        else if (config.IsSnapshot())
        {
            if (config.IsInitial())
            {
                return new ConnectRecord(config.Topic, -1, -1)
                {
                    Skip = true,
                    Deserialized = new ConnectMessage<JsonNode>
                    {
                        Value = message.ToJson()
                    }
                };
            }
            
            message.Remove("_row", out var row);

            value = new JsonObject
            {
                { "operation", "IMPORT" },
                { "timestamp", DateTime.UtcNow.ToUnixMicroseconds() },
                { "id", Convert.ToInt64(row) },
                { "old", null },
                { "new", message.ToJson() }
            };
        }
        else
        {
            value = message.ToJson();
        }
        
        var key = (value["new"]?.ToDictionary("new", true) ?? value["old"]?.ToDictionary("old", true))?
            .Where(r => config.Keys.Contains(r.Key))
            .ToDictionary(k => k.Key, v => v.Value).ToJson();

        return new ConnectRecord(config.Topic, -1, -1)
        {
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = key,
                Value = value
            }
        };
    }
}