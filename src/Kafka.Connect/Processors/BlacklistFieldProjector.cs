using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Processors
{
    public class BlacklistFieldProjector : Processor<IList<string>>
    {
        private readonly ILogger<BlacklistFieldProjector> _logger;

        public BlacklistFieldProjector(ILogger<BlacklistFieldProjector> logger, IConfigurationProvider configurationProvider) : base(configurationProvider)
        {
            _logger = logger;
        }

        protected override Task<ConnectMessage<IDictionary<string, object>>> Apply(IList<string> settings, ConnectMessage<IDictionary<string, object>> message)
        {
            using (_logger.Track("Applying blacklist field projector."))
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

        private static IDictionary<string, object> ApplyInternal(IDictionary<string, object> flattened,
            IEnumerable<string> fields = null)
        {
            fields.GetMatchingKeys(flattened).ToList().Where(flattened.ContainsKey).ForEach(key => flattened.Remove(key));
            return flattened;
        }
    }
}