using System.Collections.Generic;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Strategies;

public interface IStrategySelector
{
    IStrategy GetStrategy(ConnectRecord record, IDictionary<string, string> settings = null);
}