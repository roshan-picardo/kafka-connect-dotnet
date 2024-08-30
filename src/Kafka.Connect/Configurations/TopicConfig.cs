using System.Collections.Generic;

namespace Kafka.Connect.Configurations;

public class TopicConfig
{
    public TopicType Purpose { get; set; }
    public ConverterConfig Converters { get; set; }
    public IDictionary<int, ProcessorConfig> Processors { get; set; }
}