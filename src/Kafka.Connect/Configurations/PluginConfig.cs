using System.Collections.Generic;

namespace Kafka.Connect.Configurations
{
    public class PluginConfig
    {
        public string Location { get; set; }
        public IDictionary<string, InitializerConfig> Initializers { get; init; }
    }
}