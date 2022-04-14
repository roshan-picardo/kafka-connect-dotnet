using System.Collections.Generic;
using Kafka.Connect.Config.Models;

namespace Kafka.Connect.Configurations
{
    public class PluginConfig
    {
        public string Name { get; set; }
        public string Directory { get; set; }
        public string Assembly { get; set; }
        public string Initializer { get; set; }
    }
}