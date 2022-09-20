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
        public BlacklistFieldProjector(IConfigurationProvider configurationProvider) : base(configurationProvider)
        {
        }

        [OperationLog("Applying blacklist field projector.")]
        protected override Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, IList<string> settings)
        {
            return Task.FromResult(ApplyInternal(flattened, settings?.Select(s=> s.Prefix())));
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