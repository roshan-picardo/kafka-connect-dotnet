using System.Collections.Generic;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Strategies;

public interface IReadWriteStrategySelector
{
    IReadWriteStrategy GetReadWriteStrategy(IConnectRecord record, IDictionary<string, string> overrides);
}