using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Handlers
{
    public class MessageHandler : IMessageHandler
    {
        private readonly ILogger<MessageHandler> _logger;
        private readonly IRecordFlattener _recordFlattener;
        private readonly IProcessorServiceProvider _processorServiceProvider;
        private readonly IConfigurationProvider _configurationProvider;

        public MessageHandler(ILogger<MessageHandler> logger, IRecordFlattener recordFlattener,
            IProcessorServiceProvider processorServiceProvider, IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _recordFlattener = recordFlattener;
            _processorServiceProvider = processorServiceProvider;
            _configurationProvider = configurationProvider;
        }

        public async Task<(bool, JToken)> Process(SinkRecord record, string connector)
        {
            var configs = _configurationProvider.GetMessageProcessors(connector, record.Topic);
            if (!(configs?.Any() ?? false))
            {
                return (record.Skip, record.Data);
            }

            var processors = _processorServiceProvider.GetProcessors()?.ToList();
            if (!(processors?.Any() ?? false))
            {
                return (record.Skip, record.Data);
            }
            var flattened = _logger.Timed("Flattening the record.").Execute(() => _recordFlattener.Flatten(record.Data));
            var skip = false;
            foreach (var config in configs.OrderBy(p => p.Order))
            {
                var processor = processors.SingleOrDefault(p => p.IsOfType(config.Name));
                if (processor == null)
                {
                    _logger.LogTrace("{@Log}", new {Message = "Processor is not registered.", Processor = config.Name});
                    continue;
                }

                (skip, flattened) = await _logger.Timed($"Processing message using {processor.GetType().FullName}.")
                    .Execute( () => processor.Apply(flattened, connector));
                if (!skip) continue;
                _logger.LogTrace("{@Log}", new { Message = "Message will be skipped from further processing."});
                break;
            }
            var unflattened = _logger.Timed("Unflattening the record.").Execute(() => _recordFlattener.Unflatten(flattened));
            return (skip, unflattened);
        }
        
        public async Task<(bool, JToken)> Process(SinkRecord record, ConnectorConfig config)
        {
            if (config?.Processors == null || !config.Processors.Any())
            {
                return (record.Skip, record.Data);
            }
            var processors = _processorServiceProvider.GetProcessors()?.ToList();
            if (processors == null || !processors.Any())
            {
                return (record.Skip, record.Data);
            }
            
            var flattened = _logger.Timed("Flattening the record.").Execute(() => _recordFlattener.Flatten(record.Data));
            var skip = false;
            foreach (var processorConfig in config.Processors.OrderBy(p => p.Order))
            {
                // skip for this topic if not set
                if (processorConfig.Topics != null && !processorConfig.Topics.Contains(record.Topic))
                {
                    _logger.LogTrace("{@Log}", new {Message = "Processor is not configured for this topic.", Processor = processorConfig.Name});
                    continue;
                }
                var processor = processors.SingleOrDefault(p => p.IsOfType(processorConfig.Name));
                if (processor == null)
                {
                    _logger.LogTrace("{@Log}", new {Message = "Processor is not registered.", Processor = processorConfig.Name});
                    continue;
                }

                (skip, flattened) = await _logger.Timed($"Processing message using {processor.GetType().FullName}.")
                    .Execute(async () => await processor.Apply(flattened, config.Name));
                if (!skip) continue;
                _logger.LogTrace("{@Log}", new { Message = "Message will be skipped from further processing."});
                break;
            }

            var unflattened = _logger.Timed("Unflattening the record.").Execute(() => _recordFlattener.Unflatten(flattened));
            return (skip, unflattened);
        }
    }
}