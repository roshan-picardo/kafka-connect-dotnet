using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Strategies;

public class TopicStrategySelector : IWriteStrategySelector
{
    private readonly IEnumerable<IWriteStrategy> _writeStrategies;

    public TopicStrategySelector(IEnumerable<IWriteStrategy> writeStrategies)
    {
        _writeStrategies = writeStrategies;
    }

    public IWriteStrategy GetWriteStrategy(Plugin.Models.SinkRecord record, IDictionary<string, string> overrides)
    {
        if (overrides?.All(o => o.Key != record.Topic) ?? true)
        {
            return null;
        }

        return _writeStrategies.SingleOrDefault(s => s.GetType().FullName == overrides[record.Topic]);
    }
}