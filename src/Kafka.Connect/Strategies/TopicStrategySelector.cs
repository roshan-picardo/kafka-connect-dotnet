using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Strategies;

public class TopicStrategySelector : IStrategySelector
{
    private readonly IEnumerable<IQueryStrategy> _readWriteStrategies;

    public TopicStrategySelector(IEnumerable<IQueryStrategy> readWriteStrategies)
    {
        _readWriteStrategies = readWriteStrategies;
    }

    public IQueryStrategy GetQueryStrategy(Plugin.Models.IConnectRecord record, IDictionary<string, string> overrides)
    {
        if (overrides?.All(o => o.Key != record.Topic) ?? true)
        {
            return null;
        }

        return _readWriteStrategies.SingleOrDefault(s => s.GetType().FullName == overrides[record.Topic]);
    }
}