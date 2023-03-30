using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Serializers;

namespace Kafka.Connect.Providers
{
    public class ProcessorServiceProvider : IProcessorServiceProvider
    {
        private readonly ILogger<ProcessorServiceProvider> _logger;
        private readonly IEnumerable<IProcessor> _processors;
        private readonly IEnumerable<IDeserializer> _deserializers;

        public ProcessorServiceProvider(ILogger<ProcessorServiceProvider> logger, IEnumerable<IProcessor> processors,
            IEnumerable<IDeserializer> deserializers)
        {
            _logger = logger;
            _processors = processors;
            _deserializers = deserializers;
        }

        public IEnumerable<IProcessor> GetProcessors()
        {
            return _processors;
        }

        public IDeserializer GetDeserializer(string typeName)
        {
            var deserializer = _deserializers.SingleOrDefault(d => d.IsOfType(typeName));
            _logger.Trace($"Configured deserializer: {deserializer?.GetType().FullName}");
            return deserializer;
        }
    }
}