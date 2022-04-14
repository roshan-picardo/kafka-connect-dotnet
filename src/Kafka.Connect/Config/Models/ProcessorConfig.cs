using System.Collections.Generic;

namespace Kafka.Connect.Config.Models
{
    public class ProcessorConfig
    {
        public string Name { get; init; }
        public int Order { get; set; }
        public IDictionary<string, string> Properties { get; set; }
        public string Topic { get; init; }
        public string[] Topics { get; set; }
    }
}