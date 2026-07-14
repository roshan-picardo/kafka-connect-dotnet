using System.Text.Json;
using System.Text.Json.Nodes;
using Cassandra;
using Kafka.Connect.Cassandra.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Cassandra;

public class CassandraPluginHandler(
    IConfigurationProvider configurationProvider,
    IConnectPluginFactory connectPluginFactory,
    ICassandraCommandHandler cassandraCommandHandler,
    ICassandraClientProvider cassandraClientProvider,
    ICassandraSqlExecutor sqlExecutor,
    ILogger<CassandraPluginHandler> logger)
    : PluginHandler(configurationProvider)
{
    private readonly IConfigurationProvider _configurationProvider = configurationProvider;

    public override Task Startup(string connector) => cassandraCommandHandler.Initialize(connector);

    public override async Task<IList<ConnectRecord>> Get(string connector, int taskId, CommandRecord command)
    {
        using (logger.Track("Getting batch of records"))
        {
            var model = await connectPluginFactory.GetStrategy(connector, command).Build<string>(connector, command);
            var records = new List<ConnectRecord>();

            try
            {
                var session = cassandraClientProvider.GetCassandraClient(connector, taskId).GetSession();
                var rows = await sqlExecutor.QueryRowsAsync(session, model.Model);
                var index = command.GetCommand<CommandConfig>().Snapshot.Id;

                foreach (var row in rows)
                {
                    index++;
                    records.Add(GetConnectRecord(row, command, index));
                }
            }
            catch (Exception ex)
            {
                command.Exception = ex;
                if (records.Count == 0)
                {
                    throw new ConnectDataException(ex.Message, ex);
                }
            }

            return records;
        }
    }

    public override async Task Put(IList<ConnectRecord> records, string connector, int taskId)
    {
        using (logger.Track("Putting batch of records"))
        {
            var parallelRetryOptions = _configurationProvider.GetParallelRetryOptions(connector);
            await records.ForEachAsync(parallelRetryOptions, async cr =>
            {
                using (ConnectLog.TopicPartitionOffset(cr.Topic, cr.Partition, cr.Offset))
                {
                    if (cr is ConnectRecord { Sinking: true } record)
                    {
                        var model = await connectPluginFactory.GetStrategy(connector, record).Build<string>(connector, record);
                        record.SetModel(model.Model);
                        record.Status = model.Status;
                    }
                }
            });

            var session = cassandraClientProvider.GetCassandraClient(connector, taskId).GetSession();
            parallelRetryOptions.DegreeOfParallelism = 1;

            await records
                .OrderBy(r => r.Topic)
                .ThenBy(r => r.Partition)
                .ThenBy(r => r.Offset)
                .ForEachAsync(parallelRetryOptions, async cr =>
                {
                    using (ConnectLog.TopicPartitionOffset(cr.Topic, cr.Partition, cr.Offset))
                    {
                        if (cr is ConnectRecord { Saving: true } record)
                        {
                            await sqlExecutor.ExecuteAsync(session, record.GetModel<string>());
                        }
                    }
                });
        }
    }

    public override IDictionary<string, Command> Commands(string connector) => cassandraCommandHandler.Get(connector);

    public override JsonNode NextCommand(CommandRecord command, List<ConnectRecord> records) =>
        cassandraCommandHandler.Next(command,
            records.Where(r => r.Status is Status.Published or Status.Skipped or Status.Triggered)
                .Select(r => r.Deserialized)
                .ToList());

    public override Task Purge(string connector) => cassandraCommandHandler.Purge(connector);

    private static ConnectRecord GetConnectRecord(Dictionary<string, object> message, CommandRecord command, long index)
    {
        var config = command.GetCommand<CommandConfig>();

        var normalizedAfter = message
            .ToDictionary(k => k.Key, v => JsonValue.Create(v.Value?.ToString()) as JsonNode);

        var value = new JsonObject
        {
            ["id"] = index,
            ["operation"] = "CHANGE",
            ["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000,
            ["before"] = new JsonObject(),
            ["after"] = JsonSerializer.SerializeToNode(normalizedAfter)
        };

        return new ConnectRecord(config.Topic, -1, -1)
        {
            Status = Status.Selected,
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = BuildKey(value["after"] as JsonObject, config.Keys),
                Value = value
            }
        };
    }

    private static JsonNode? BuildKey(JsonObject? after, string[] keys)
    {
        if (after == null || keys.Length == 0)
        {
            return null;
        }

        var key = new JsonObject();
        foreach (var k in keys)
        {
            if (after.TryGetPropertyValue(k, out var value))
            {
                key[k] = value?.DeepClone();
            }
        }

        return key;
    }
}
