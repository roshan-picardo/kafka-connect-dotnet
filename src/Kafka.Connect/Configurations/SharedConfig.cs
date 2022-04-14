using System.Collections.Generic;

namespace Kafka.Connect.Configurations
{
    public class SharedConfig
    {
        public ErrorsConfig Errors { get; set; }
        public RetryConfig Retries { get; set; }
        public EofConfig EofSignal { get; set; }
        public BatchConfig Batch { get; set; }
        
        public ConverterConfig Deserializers { get; set; }
        public IEnumerable<ProcessorConfig> Processors { get; set; }
    }
}