using System.Collections.Generic;

namespace Kafka.Connect.Configurations;

public class StrategyConfig
{
    public string Name { get; init; }
    public StrategySelectorConfig Selector { get; init; }
}

public class StrategySelectorConfig
{
    public string Name { get; init; }
    public IDictionary<string, string> Overrides { get; init; }
}
