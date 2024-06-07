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
            var model = await GetReadWriteStrategy(connector, command).Build<string>(connector, command);
            var commandConfig = command.GetCommand<CommandConfig>();

            await using (var reader = await new NpgsqlCommand(model.Model,
                                 postgresClientProvider.GetPostgresClient(connector, taskId).GetConnection())
                             .ExecuteReaderAsync())
            {
                var records = new List<ConnectRecord>();
                while (await reader.ReadAsync())
                {
                    var record = new Dictionary<string, object>();
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        record.Add(reader.GetName(i), reader.GetValue(i));
                    }

                    records.Add(new ConnectRecord(commandConfig.Topic, -1, -1)
                        { Deserialized = GetConnectMessage(record, commandConfig) });
                }

                await reader.CloseAsync();
                return records;
            }
        }
    }

    public override IDictionary<string, Command> GetCommands(string connector)
    {
        var config = _configurationProvider.GetPluginConfig<SourceConfig>(connector);
        return config.Commands.ToDictionary(k=> k.Key, v => v.Value as Command);
    }

    public override CommandRecord GetUpdatedCommand(CommandRecord command, IList<ConnectMessage<JsonNode>> records)
    {
        var commandConfig = command.GetCommand<CommandConfig>();
        if (records.Any())
        {
            var maxTimestamp = records.Max(m => m.Timestamp);

            var keys = records.Where(m => m.Timestamp == maxTimestamp)
                .Select(m => m.Key.ToDictionary()).OrderBy(_ => 1);
            foreach (var keyColumn in commandConfig.KeyColumns)
            {
                keys = keys.ThenBy(d => d[keyColumn]);
            }
            commandConfig.Timestamp = maxTimestamp;
            commandConfig.Keys = keys.LastOrDefault();
        }
        command.Command = commandConfig.ToJson();
        return command;
    }

    private static ConnectMessage<JsonNode> GetConnectMessage(IDictionary<string, object> record, CommandConfig command)
    {
        long timestamp;
        if(record.TryGetValue("_timestamp", out var value))
        {
            timestamp = Convert.ToInt64(value);
            record.Remove("_timestamp");
        }
        else
        {
            timestamp = record[command.TimestampColumn] is DateTime dateTime
                ? dateTime.Ticks
                : (long)record[command.TimestampColumn];
        }
        
        return new ConnectMessage<JsonNode>
        {
            Timestamp = timestamp,
            Key = JsonNode.Parse(JsonSerializer.Serialize(record
                .Where(r => command.KeyColumns?.Contains(r.Key) ?? false).ToDictionary(k => k.Key, v => v.Value))),
            Value = record.ToJson()
        };
    }
}