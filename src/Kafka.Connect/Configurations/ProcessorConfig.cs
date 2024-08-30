using System.Collections.Generic;

namespace Kafka.Connect.Configurations
{
    public class ProcessorConfig
    {
        public string Name { get; set; }
        
    }

    public class ProcessorConfig<T> : ProcessorConfig
    {
        public T Settings { get; init; }
    }
    
    public class ConnectorConfig<T> : ConnectorConfig
    {
        public new IDictionary<string, ProcessorConfig<T>> Processors { get; init; }
    }
}