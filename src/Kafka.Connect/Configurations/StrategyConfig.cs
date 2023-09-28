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


// { sink: { handler: "", strategy: { name: "", selector: {name: "", overrides: { "one" : "this"} } } }