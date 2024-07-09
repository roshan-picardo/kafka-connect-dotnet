using System.Collections.Generic;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Strategies;

public interface IStrategySelector
{
    IStrategy GetQueryStrategy(IConnectRecord record, IDictionary<string, string> overrides);
}