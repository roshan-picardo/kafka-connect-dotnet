using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Microsoft.Extensions.Options;

namespace Kafka.Connect.Processors
{
    public class BlacklistFieldProjector : Processor<IList<string>>
    {
        public BlacklistFieldProjector(IOptions<List<ConnectorConfig<IList<string>>>> options, IOptions<ConnectorConfig<IList<string>>> shared) : base(options, shared)
        {
        }

        [OperationLog("Applying blacklist field projector.")]
        protected override Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, IList<string> settings)
        {
            return Task.FromResult(ApplyInternal(flattened, settings?.Select(ProcessorHelper.PrefixValue)));
        }

        private static (bool, IDictionary<string, object>) ApplyInternal(IDictionary<string, object> flattened,
            IEnumerable<string> fields = null)
        {
            foreach (var key in ProcessorHelper.GetKeys(flattened, fields).ToList().Where(flattened.ContainsKey))
            {
                flattened.Remove(key);
            }

            return (false, flattened);
        }
    }
}