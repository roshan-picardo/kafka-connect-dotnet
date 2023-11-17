using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Handlers
{
    public class MessageHandler : IMessageHandler
    {
        private readonly ILogger<MessageHandler> _logger;
        private readonly IProcessorServiceProvider _processorServiceProvider;
        private readonly IConfigurationProvider _configurationProvider;

        public MessageHandler(
            ILogger<MessageHandler> logger,
            IProcessorServiceProvider processorServiceProvider,
            IConfigurationProvider configurationProvider)
        {
            _logger = logger;
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
                    return (record.Skip, record.DeserializedToken);
                }

                var processors = _processorServiceProvider.GetProcessors()?.ToList();
                if (!(processors?.Any() ?? false))
                {
                    return (record.Skip, record.DeserializedToken);
                }

                record.Flattened = new ConnectMessage<IDictionary<string, object>>
                {
                    Key = record.DeserializedToken.Key?.ToJsonNode().ToDictionary(),
                    Value = record.DeserializedToken.Value?.ToJsonNode().ToDictionary()
                };

                foreach (var config in configs.OrderBy(p => p.Order))
                {
                    var processor = processors.SingleOrDefault(p => p.IsOfType(config.Name));
                    if (processor == null)
                    {
                        _logger.Trace("Processor is not registered.", new { Processor = config.Name });
                        continue;
                    }

                    record.Flattened = await processor.Apply(connector, record.Flattened);
                    if (!record.Flattened.Skip) continue;
                    _logger.Trace("Message will be skipped from further processing.");
                    break;
                }

                return (record.Flattened.Skip,
                    new ConnectMessage<JToken>
                    {
                        Key = record.Flattened.Key.ToJson().ToJToken(),
                        Value = record.Flattened.Value.ToJson().ToJToken()
                    });
            }
        }
    }
}