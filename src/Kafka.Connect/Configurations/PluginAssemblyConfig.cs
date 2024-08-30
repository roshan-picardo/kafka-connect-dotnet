using System.Collections.Generic;

namespace Kafka.Connect.Configurations;

public class PluginAssemblyConfig
{
    public string Location { get; set; }
    public IDictionary<string, InitializerConfig> Initializers { get; init; }
}
    
public class InitializerConfig
{
    public string Prefix { get; set; }
    public string Assembly { get; init; }
    public string Class { get; init; }
}