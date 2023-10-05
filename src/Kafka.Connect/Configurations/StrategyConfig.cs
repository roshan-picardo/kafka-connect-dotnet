using System.Collections.Generic;

namespace Kafka.Connect.Configurations;

public class StrategyConfig
{
    public string Name { get; set; }
    public StrategySelectorConfig Selector { get; set; }
    public bool SkipOnFailure { get; set; }
}

public class StrategySelectorConfig
{
    public string Name { get; set; }
    public IDictionary<string, string> Overrides { get; set; }
}

public class StrategySelectorConfig<T>
{
    public string Name { get; set; }
    public T Overrides { get; set; }
}

public class StrategyConfig<T>
{
    public string Name { get; set; }
    public StrategySelectorConfig<T> Selector { get; set; }
    public bool SkipOnFailure { get; set; }
}

