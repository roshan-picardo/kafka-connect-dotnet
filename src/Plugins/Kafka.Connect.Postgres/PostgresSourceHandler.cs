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

            var reader = await new NpgsqlCommand(model.Model, postgresClientProvider.GetPostgresClient(connector, taskId).GetConnection())
                .ExecuteReaderAsync();
            var records = new List<ConnectRecord>();

            while (reader.Read())
            {
                var record = new Dictionary<string, object>();
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    record.Add(reader.GetName(i), reader.GetValue(i));
                }

                records.Add(new ConnectRecord(commandConfig.Topic, -1, -1)
                    { Deserialized = GetConnectMessage(record, commandConfig) });
            }

            return records;
        }
    }

    public override IDictionary<string, Command> GetCommands(string connector)
    {
        var config = _configurationProvider.GetPluginConfig<SourceConfig>(connector);
        return config.Commands.ToDictionary(k=> k.Key, v => v.Value as Command);
    }

    public override CommandRecord GetUpdatedCommand(CommandRecord command, IList<(SinkStatus Status, JsonNode Key)> records)
    {
        var commandConfig = command.GetCommand<CommandConfig>();
        var jsonKeys = records.Where(r => r.Status is SinkStatus.Published or SinkStatus.Skipped or SinkStatus.Processed).Select(r=> r.Key).ToList();

        if (jsonKeys.Count > 0)
        {
            var maxTimestamp = jsonKeys.Select(k => k["Timestamp"]?.GetValue<long>() ?? 0).Max();
            
            var keys = jsonKeys.Where(k => k["Timestamp"]?.GetValue<long>() == maxTimestamp)
                .Select(k => k["Keys"]?.ToDictionary("Keys", true)).ToList();
            var sortedKeys = keys.OrderBy(_ => 1);
            foreach (var keyColumn in commandConfig.KeyColumns)
            {
                sortedKeys = sortedKeys.ThenBy(d => d[keyColumn]);
            }

            commandConfig.Timestamp = maxTimestamp;
            commandConfig.Keys = sortedKeys.LastOrDefault();
        }

        command.Command = commandConfig.ToJson();

        return command;
    }
    
    private static ConnectMessage<JsonNode> GetConnectMessage(IDictionary<string, object> record, CommandConfig command)
    {
        var commandKey = new
        {
            Timestamp = record[command.TimestampColumn],
            Keys = record.Where(r => command.KeyColumns?.Contains(r.Key) ?? false).ToDictionary(k => k.Key, v => v.Value)
        };
        return new ConnectMessage<JsonNode> { Key = JsonNode.Parse(JsonSerializer.Serialize(commandKey)), Value = record.ToJson() };
    }
}