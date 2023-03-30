using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
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
        
        protected override Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, IList<string> settings)
        {
            using (_logger.Track("Applying whitelist field projector."))
            {
                return Task.FromResult(ApplyInternal(flattened, settings?.Select(s => s.Prefix())));
            }
        }

        private static (bool, IDictionary<string, object>) ApplyInternal(IDictionary<string, object> flattened,
            IEnumerable<string> fields = null)
        {
            return (false, fields.GetMatchingKeys(flattened).ToList().Where(flattened.ContainsKey)
                .ToDictionary(key => key, key => flattened[key]));
        }
    }
}
