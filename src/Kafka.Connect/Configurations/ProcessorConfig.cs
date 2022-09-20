using System.Collections.Generic;

namespace Kafka.Connect.Configurations
{
    public class ProcessorConfig
    {
        public string Name { get; set; }
        public int Order { get; init; }
        public IList<string> Topics { get; init; }
        
    }

    public class ProcessorConfig<T> : ProcessorConfig
    {
        public T Settings { get; init; }
    }
}