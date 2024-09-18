using System.Collections.Generic;

namespace Kafka.Connect.Configurations;

public class StrategyConfig
{
    public string Name { get; init; }
    public string Selector { get; init; }
    public IDictionary<string, string> Settings { get; init; }
}