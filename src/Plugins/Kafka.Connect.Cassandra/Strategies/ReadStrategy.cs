using Kafka.Connect.Cassandra.Models;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Cassandra.Strategies;

public class ReadStrategy(ILogger<ReadStrategy> logger) : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
        => throw new NotImplementedException();

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating source CQL"))
        {
            var command = record.GetCommand<CommandConfig>();
            var model = new StrategyModel<string> { Status = Status.Selecting };

            var filters = command.Filters?.Where(f => f.Value != null).ToList() ?? [];
            var where = filters.Count == 0
                ? string.Empty
                : " WHERE " + string.Join(" AND ", filters.Select(f => $"\"{f.Key}\" > '{f.Value}'"));

            var orderBy = command.Filters == null || command.Filters.Count == 0
                ? string.Empty
                : $" ORDER BY {string.Join(", ", command.Filters.Keys.Select(k => $"\"{k}\""))}";

            model.Model = $"SELECT * FROM {command.Keyspace}.{command.Table}{where}{orderBy} LIMIT {record.BatchSize} ALLOW FILTERING;";
            return Task.FromResult(model);
        }
    }
}
