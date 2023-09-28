using System.Collections.Generic;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Strategies;

public interface IWriteStrategySelector
{
    IWriteStrategy GetWriteStrategy(SinkRecord record, IDictionary<string, string> overrides);
}