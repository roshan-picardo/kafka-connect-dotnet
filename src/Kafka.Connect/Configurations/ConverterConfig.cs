using System.Collections.Generic;

namespace Kafka.Connect.Configurations
{
    public class ConverterConfig
    {
        public string Key { get; init; }
        public string Value { get; init; }
        public IList<ConverterOverrideConfig> Overrides { get; init; }
    }
}