using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Processors;

namespace Kafka.Connect.Providers;

public class ProcessorServiceProvider : IProcessorServiceProvider
{
    private readonly ILogger<ProcessorServiceProvider> _logger;
    private readonly IEnumerable<IProcessor> _processors;
    private readonly IEnumerable<IMessageConverter> _converters;

    public ProcessorServiceProvider(
        ILogger<ProcessorServiceProvider> logger,
        IEnumerable<IProcessor> processors,
        IEnumerable<IMessageConverter> converters)
    {
        _logger = logger;
        _processors = processors;
        _converters = converters;
    }

    public IEnumerable<IProcessor> GetProcessors() => _processors;

    public IMessageConverter GetMessageConverter(string typeName) => _converters.SingleOrDefault(c => c.Is(typeName)); 
}
