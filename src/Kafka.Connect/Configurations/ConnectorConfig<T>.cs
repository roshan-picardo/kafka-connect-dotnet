using System.Collections.Generic;
using System.Linq;

namespace Kafka.Connect.Configurations;

public class ConnectorConfig<T> : ConnectorConfig
{
    private readonly IDictionary<string, ProcessorConfig<T>> _processors;

    public new IDictionary<string, ProcessorConfig<T>> Processors
    {
        get
        {
            if (_processors == null || !_processors.Any())
            {
                return _processors;
            }

            foreach (var (name, processor) in _processors)
            {
                if (processor != null && string.IsNullOrEmpty(processor.Name))
                {
                    processor.Name = name;
                }
            }

            return _processors;
        }
        init => _processors = value;
    }
}