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

public class PostgresPluginHandler(
    IConfigurationProvider configurationProvider,
    IConnectPluginFactory connectPluginFactory,
    IPostgresCommandHandler postgresCommandHandler,
    IPostgresClientProvider postgresClientProvider,
    ILogger<PostgresPluginHandler> logger)
    : PluginHandler(configurationProvider)
{
    private readonly IConfigurationProvider _configurationProvider = configurationProvider;

    public override Task Startup(string connector) => postgresCommandHandler.Initialize(connector);

    public override async Task<IList<ConnectRecord>> Get(string connector, int taskId, CommandRecord command)
    {
        using (logger.Track("Getting batch of records"))
        {
            var changeLog = _configurationProvider.GetPluginConfig<PluginConfig>(connector).Changelog;
            command.Changelog = JsonSerializer.SerializeToNode(changeLog);
            var model = await connectPluginFactory.GetStrategy(connector, command).Build<string>(connector, command);

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

                records.Add(GetConnectRecord(record, command));
            }

            await reader.CloseAsync();

            return records;
        }
    }

    public override async Task Put(IEnumerable<ConnectRecord> records, string connector, int taskId)
    {
        using (logger.Track("Putting batch of records"))
        {
            var parallelRetryOptions = _configurationProvider.GetParallelRetryOptions(connector);
            var models = new List<StrategyModel<string>>();
            await records.ForEachAsync(parallelRetryOptions, async cr =>
            {
                using (ConnectLog.TopicPartitionOffset(cr.Topic, cr.Partition, cr.Offset))
                {
                    if (cr is ConnectRecord { Sinking: true } record)
                    {
                        var model = await connectPluginFactory.GetStrategy(connector, record)
                            .Build<string>(connector, record);
                        models.Add(model);
                        record.Status = model.Status;
                    }
                }
            });


            foreach (var model in models.OrderBy(m => m.Topic)
                         .ThenBy(m => m.Partition)
                         .ThenBy(m => m.Offset)
                         .SelectMany(m => m.Models))
            {
                var command = new NpgsqlCommand(model,
                    postgresClientProvider.GetPostgresClient(connector, taskId).GetConnection());
                await command.ExecuteNonQueryAsync();
            }
        }
    }

    public override IDictionary<string, Command> Commands(string connector) => postgresCommandHandler.Get(connector);

    public override JsonNode NextCommand(CommandRecord command, List<ConnectRecord> records) =>
        postgresCommandHandler.Next(command, records.Where(r => r.Status is Status.Published or Status.Skipped or Status.Triggered)
            .Select(r => r.Deserialized).ToList());
    
    
    private static ConnectRecord GetConnectRecord(Dictionary<string, object> message, CommandRecord command)
    {
        var config = command.GetCommand<CommandConfig>();
        var skipIfInitial = command.IsChangeLog() && config.IsSnapshot() && config.IsInitial();
        var value = message.ToJson();
        if (!skipIfInitial)
        {
            if (value["before"] != null)
            {
                value["before"] = JsonNode.Parse(value["before"].ToString());
            }

            if (value["after"] != null)
            {
                value["after"] = JsonNode.Parse(value["after"].ToString());
            }
        }
        return new ConnectRecord(config.Topic, -1, -1)
        {
            Status = skipIfInitial ? Status.Triggered : Status.Selected,
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = skipIfInitial
                    ? null
                    : (value["after"]?.ToDictionary("after", true) ?? value["before"]?.ToDictionary("before", true))?
                    .Where(r => config.Keys.Contains(r.Key))
                    .ToDictionary(k => k.Key, v => v.Value)
                    .ToJson(),
                Value = value
            }
        };
    }
}
