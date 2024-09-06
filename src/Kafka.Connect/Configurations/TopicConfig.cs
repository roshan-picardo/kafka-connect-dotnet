using System.Collections.Generic;

namespace Kafka.Connect.Configurations;

public class TopicConfig
{
    public TopicType Purpose { get; init; }
    public ConverterConfig Converters { get; init; }
    public IDictionary<int, ProcessorConfig> Processors { get; init; }
}

public enum TopicType
{
    None = 0,
    Command,
    Config
}
