using System.Collections.Generic;

namespace Kafka.Connect.Configurations;

public class ConverterConfig
{
    public string Key { get; init; }
    public string Value { get; init; }
    public string Subject { get; init; }
    public string Record { get; init; }
    public IList<ConverterOverrideConfig> Overrides { get; init; }
}

public class ConverterOverrideConfig
{
    public string Key { get; init; }
    public string Value { get; init; }
    public string Topic { get; init; }
    public string Subject { get; init; }
    public string Record { get; init; }
}
