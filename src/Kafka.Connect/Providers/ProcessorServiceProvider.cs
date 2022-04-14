using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Serializers;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Providers
{
    public class ProcessorServiceProvider : IProcessorServiceProvider
    {
        private readonly ILogger<ProcessorServiceProvider> _logger;
        private readonly IEnumerable<IProcessor> _processors;
        private readonly IEnumerable<IDeserializer> _deserializers;
        private readonly IEnumerable<ISerializer> _serializers;
        private readonly IEnumerable<IEnricher> _enrichers;

        public ProcessorServiceProvider(ILogger<ProcessorServiceProvider> logger, IEnumerable<IProcessor> processors,
            IEnumerable<IDeserializer> deserializers, IEnumerable<ISerializer> serializers, IEnumerable<IEnricher> enrichers)
        {
            _logger = logger;
            _processors = processors;
            _deserializers = deserializers;
            _serializers = serializers;
            _enrichers = enrichers;
        }

        public IEnumerable<IProcessor> GetProcessors()
        {
            return _processors;
        }

        public IDeserializer GetKeyDeserializer(ConverterConfig config)
        {
            var deserializer = _deserializers.SingleOrDefault(d => d.IsOfType(config.Key));
            _logger.LogTrace("{@Log}", new {Message = $"Key deserializer{deserializer?.GetType().FullName}"});
            return deserializer;
        }

        public IDeserializer GetValueDeserializer(ConverterConfig config)
        {
            var deserializer = _deserializers.SingleOrDefault(d => d.IsOfType(config.Value));
            _logger.LogTrace("{@Log}", new {Message = $"Value deserializer{deserializer?.GetType().FullName}"});
            return deserializer;
        }

        public IDeserializer GetDeserializer(string typeName)
        {
            var deserializer = _deserializers.SingleOrDefault(d => d.IsOfType(typeName));
            _logger.LogTrace("{@Log}", new {Message = $"Configured deserializer{deserializer?.GetType().FullName}"});
            return deserializer;
        }

        public ISerializer GetSerializer(string typeName)
        {
            var serializer = _serializers.SingleOrDefault(d => d.IsOfType(typeName));
            _logger.LogTrace("{@Log}", new {Message = $"Configured serializer{serializer?.GetType().FullName}"});
            return serializer;
        }

        public IEnumerable<IEnricher> GetEnrichers()
        {
            return _enrichers;
        }
    }
}