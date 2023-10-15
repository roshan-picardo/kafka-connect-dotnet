using System.Collections.Generic;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Strategies;

public interface IWriteStrategySelector
{
    IWriteStrategy GetWriteStrategy(ConnectRecord record, IDictionary<string, string> overrides);
}