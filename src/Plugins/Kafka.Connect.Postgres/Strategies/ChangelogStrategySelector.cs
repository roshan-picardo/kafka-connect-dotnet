using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Postgres.Strategies;

public class ChangelogStrategySelector(IEnumerable<IStrategy> strategies) : IStrategySelector
{
    public IStrategy GetStrategy(ConnectRecord record, IDictionary<string, string> settings)
    {
        var operation = record.Raw.GetValue<string>("operation");
        return operation switch
        {
            "IMPORT" => strategies.SingleOrDefault(s => s.Is<UpsertStrategy>()),
            "CHANGE" => strategies.SingleOrDefault(s => s.Is<UpsertStrategy>()),
            "INSERT" => strategies.SingleOrDefault(s => s.Is<InsertStrategy>()),
            "UPDATE" => strategies.SingleOrDefault(s => s.Is<UpdateStrategy>()),
            "DELETE" => strategies.SingleOrDefault(s => s.Is<DeleteStrategy>()),
            _ => strategies.SingleOrDefault(s => s.Is("Kafka.Connect.Strategies.SkipStrategy"))
        };
    }
}
