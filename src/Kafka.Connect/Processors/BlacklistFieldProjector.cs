using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
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

        protected override Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, IList<string> settings)
        {
            using (_logger.Track("Applying blacklist field projector."))
            {
                return Task.FromResult(ApplyInternal(flattened, settings?.Select(s => s.Prefix())));
            }
        }

        private static (bool, IDictionary<string, object>) ApplyInternal(IDictionary<string, object> flattened,
            IEnumerable<string> fields = null)
        {
            foreach (var key in fields.GetMatchingKeys(flattened).ToList().Where(flattened.ContainsKey))
            {
                flattened.Remove(key);
            }

            return (false, flattened);
        }
    }
}