using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Serializers;

namespace Kafka.Connect.Providers;

public class ProcessorServiceProvider : IProcessorServiceProvider
{
    private readonly ILogger<ProcessorServiceProvider> _logger;
    private readonly IEnumerable<IProcessor> _processors;
    private readonly IEnumerable<IDeserializer> _deserializers;
    private readonly IEnumerable<ISerializer> _serializers;

    public ProcessorServiceProvider(
        ILogger<ProcessorServiceProvider> logger,
        IEnumerable<IProcessor> processors,
        IEnumerable<IDeserializer> deserializers,
        IEnumerable<ISerializer> serializers)
    {
        _logger = logger;
        _processors = processors;
        _deserializers = deserializers;
        _serializers = serializers;
    }

    public IEnumerable<IProcessor> GetProcessors()
    {
        return _processors;
    }

    public IDeserializer GetDeserializer(string typeName)
    {
        var deserializer = _deserializers.SingleOrDefault(d => d.Is(typeName));
        _logger.Trace($"Configured deserializer: {deserializer?.GetType().FullName}");
        return deserializer;
    }
        
    public ISerializer GetSerializer(string typeName)
    {
        var serializer = _serializers.SingleOrDefault(d => d.Is(typeName));
        _logger.Trace($"Configured deserializer: {serializer?.GetType().FullName}");
        return serializer;
    }
}
