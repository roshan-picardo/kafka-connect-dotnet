using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Microsoft.Extensions.Options;

namespace Kafka.Connect.Processors
{
    public class FieldRenamer : Processor<IDictionary<string, string>>
    {
        public FieldRenamer(IOptions<List<ConnectorConfig<IDictionary<string, string>>>> options, IOptions<ConnectorConfig<IDictionary<string, string>>> shared) : base(options, shared)
        {
        }

        protected override Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, IDictionary<string, string> settings)
        {
            return Task.FromResult(ApplyInternal(flattened, settings?.ToDictionary(k => ProcessorHelper.PrefixValue(k.Key), v => v.Value)));
        }

        private static (bool, IDictionary<string, object>) ApplyInternal(IDictionary<string, object> flattened, IDictionary<string, string> maps = null)
        {
            var renamed = new Dictionary<string, object>();
            foreach (var (key, value) in ProcessorHelper.GetMaps(flattened, maps ?? new Dictionary<string, string>()).ToList())
            {
                if (flattened[key] == null || !(flattened[key] is { } o)) continue;
                renamed.Add(ProcessorHelper.PrefixValue(value), o);
                flattened.Remove(key);
            }

            foreach (var (key, value) in flattened)
            {
                renamed.Add(key, value);
            }
            return (false, renamed);
        }
    }
}