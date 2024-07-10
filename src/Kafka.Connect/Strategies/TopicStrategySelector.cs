using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Strategies;

public class TopicStrategySelector(IEnumerable<IStrategy> strategies) : IStrategySelector
{
    public IStrategy GetQueryStrategy(Plugin.Models.IConnectRecord record, IDictionary<string, string> overrides)
    {
        if (overrides?.All(o => o.Key != record.Topic) ?? true)
        {
            return null;
        }

        return strategies.SingleOrDefault(s => s.GetType().FullName == overrides[record.Topic]);
    }
}