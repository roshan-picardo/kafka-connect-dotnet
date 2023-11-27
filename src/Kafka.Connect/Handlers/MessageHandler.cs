using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;

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

        public async Task<ConnectMessage<JsonNode>> Process(string connector, string topic,  ConnectMessage<IDictionary<string, object>> flattened)
        {
            using (_logger.Track("Processing the message."))
            {
                var configs = _configurationProvider.GetMessageProcessors(connector, topic);
                if (!(configs?.Any() ?? false))
                {
                    return new ConnectMessage<JsonNode>
                    {
                        Skip = flattened.Skip,
                        Key = flattened.Key.ToJson(),
                        Value = flattened.Key.ToJson()
                    };
                }

                var processors = _processorServiceProvider.GetProcessors()?.ToList();
                if (!(processors?.Any() ?? false))
                {
                    return new ConnectMessage<JsonNode>
                    {
                        Skip = flattened.Skip,
                        Key = flattened.Key.ToJson(),
                        Value = flattened.Key.ToJson()
                    };
                }

                foreach (var config in configs.OrderBy(p => p.Order))
                {
                    var processor = processors.SingleOrDefault(p => p.Is(config.Name));
                    if (processor == null)
                    {
                        _logger.Trace("Processor is not registered.", new { Processor = config.Name });
                        continue;
                    }

                    flattened = await processor.Apply(connector, flattened);
                    if (!flattened.Skip) continue;
                    _logger.Trace("Message will be skipped from further processing.");
                    break;
                }

                return new ConnectMessage<JsonNode>
                {
                    Key = flattened.Key.ToJson(),
                    Value = flattened.Value.ToJson()
                };
            }
        }
    }
}