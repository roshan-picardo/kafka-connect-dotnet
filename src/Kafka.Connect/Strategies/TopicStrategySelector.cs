using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Strategies;

public class TopicStrategySelector(IEnumerable<IStrategy> strategies) : IStrategySelector
{
    public IStrategy GetStrategy(ConnectRecord record, IDictionary<string, string> settings)
    {
        if (settings?.All(o => o.Key != record.Topic) ?? true)
        {
            return null;
        }

        return strategies.SingleOrDefault(s => s.GetType().FullName == settings[record.Topic]);
    }
}