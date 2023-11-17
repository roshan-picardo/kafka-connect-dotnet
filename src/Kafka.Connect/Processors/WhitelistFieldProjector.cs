using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Processors
{
    public class WhitelistFieldProjector : Processor<IList<string>>
    {
        private readonly ILogger<WhitelistFieldProjector> _logger;

        public WhitelistFieldProjector(ILogger<WhitelistFieldProjector> logger, IConfigurationProvider configurationProvider) : base(configurationProvider)
        {
            _logger = logger;
        }

        protected override Task<ConnectMessage<IDictionary<string, object>>> Apply(IList<string> settings, ConnectMessage<IDictionary<string, object>> message)
        {
            using (_logger.Track("Applying whitelist field projector."))
            {
                var processed = new ConnectMessage<IDictionary<string, object>>
                {
                    Skip = false,
                    Key = ApplyInternal(message.Key, settings?.Where(s => s.StartsWith("key"))),
                    Value = ApplyInternal(message.Value, settings?.Where(s => !s.StartsWith("key"))),
                };
                return Task.FromResult(processed);
            }
        }

        protected override Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, IList<string> settings)
        {
            using (_logger.Track("Applying whitelist field projector."))
            {
                return Task.FromResult((false, ApplyInternal(flattened, settings?.Select(s => s.Prefix()))));
            }
        }

        private static IDictionary<string, object> ApplyInternal(IDictionary<string, object> flattened,
            IEnumerable<string> fields = null)
        {
            return fields.GetMatchingKeys(flattened).ToList().Where(flattened.ContainsKey)
                .ToDictionary(key => key, key => flattened[key]);
        }
    }
}
