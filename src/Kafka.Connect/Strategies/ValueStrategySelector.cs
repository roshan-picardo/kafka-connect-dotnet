using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Strategies;

public class ValueStrategySelector : IWriteStrategySelector
{
    private readonly IEnumerable<IWriteStrategy> _writeStrategies;

    public ValueStrategySelector(IEnumerable<IWriteStrategy> writeStrategies)
    {
        _writeStrategies = writeStrategies;
    }
    public IWriteStrategy GetWriteStrategy(Plugin.Models.ConnectRecord record, IDictionary<string, string> overrides)
    {
        if (overrides == null || !overrides.Any())
        {
            return null;
        }

        var values = record.DeserializedToken.Value.ToObject<IDictionary<string, object>>().Select(d => $"{d.Key}={d.Value}").ToList();
        return (from @override in overrides
                where values.Contains(@override.Key)
                select _writeStrategies.SingleOrDefault(s => s.GetType().FullName == overrides[@override.Key]))
            .FirstOrDefault();
    }
}