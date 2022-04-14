using System.Collections.Generic;

namespace Kafka.Connect.Plugin.Models
{
    public class ConnectorConfig<T>
    {
        public string Name { get; set; }
        public List<ProcessorConfig<T>> Processors { get; set; }
    }
}