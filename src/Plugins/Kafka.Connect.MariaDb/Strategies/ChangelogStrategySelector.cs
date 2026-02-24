using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.MariaDb.Strategies;

public class ChangelogStrategySelector(IEnumerable<IStrategy> strategies, ILogger<ChangelogStrategySelector> logger)
    : IStrategySelector
{
    public IStrategy GetStrategy(ConnectRecord record, IDictionary<string, string> settings = null)
    {
        using (logger.Track("Selecting strategy based on operation"))
        {
            var operation = record.Raw.GetValue<string>("operation");
            return operation switch
            {
                "INSERT" => strategies.SingleOrDefault(s => s.Is<InsertStrategy>()),
                "UPDATE" => strategies.SingleOrDefault(s => s.Is<UpdateStrategy>()),
                "DELETE" => strategies.SingleOrDefault(s => s.Is<DeleteStrategy>()),
                "IMPORT" => strategies.SingleOrDefault(s => s.Is<UpsertStrategy>()),
                "CHANGE" => strategies.SingleOrDefault(s => s.Is<UpsertStrategy>()),
                _ => strategies.SingleOrDefault(s => s.Is("Kafka.Connect.Strategies.SkipStrategy"))
            };
        }
    }
}
