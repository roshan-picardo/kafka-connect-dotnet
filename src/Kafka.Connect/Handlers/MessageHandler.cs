using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
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

        public async Task<(bool, ConnectMessage<JToken>)> Process(ConnectRecord record, string connector)
        {
            using (_logger.Track("Processing the message."))
            {
                var configs = _configurationProvider.GetMessageProcessors(connector, record.Topic);
                if (!(configs?.Any() ?? false))
                {
                    return (record.Skip, record.Deserialized);
                }

                var processors = _processorServiceProvider.GetProcessors()?.ToList();
                if (!(processors?.Any() ?? false))
                {
                    return (record.Skip, record.Deserialized);
                }

                var flattened = _recordFlattener.Flatten(new JObject
                    { { "Key", record.Deserialized.Key }, { "Value", record.Deserialized.Value } });
                var skip = false;
                foreach (var config in configs.OrderBy(p => p.Order))
                {
                    var processor = processors.SingleOrDefault(p => p.IsOfType(config.Name));
                    if (processor == null)
                    {
                        _logger.Trace("Processor is not registered.", new { Processor = config.Name });
                        continue;
                    }

                    (skip, flattened) = await processor.Apply(flattened, connector);
                    if (!skip) continue;
                    _logger.Trace("Message will be skipped from further processing.");
                    break;
                }

                var unflattened = _recordFlattener.Unflatten(flattened);
                return (skip, new ConnectMessage<JToken> { Key = unflattened?["Key"], Value = unflattened?["Value"]});
            }
        }
    }
}