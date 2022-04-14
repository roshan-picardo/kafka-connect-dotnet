using System.Collections.Generic;

namespace Kafka.Connect.Config.Models
{
    public class PluginConfig
    {
        public string Name { get; set; }
        public string Directory { get; set; }
        public IEnumerable<InitializerConfig> Initializers { get; set; }
    }
}