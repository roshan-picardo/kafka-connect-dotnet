using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Processors
{
    public class FieldRenamer : Processor<IDictionary<string, string>>
    {
        private readonly ILogger<FieldRenamer> _logger;

        public FieldRenamer(ILogger<FieldRenamer> logger, IConfigurationProvider configurationProvider) : base(configurationProvider)
        {
            _logger = logger;
        }

        protected override Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, IDictionary<string, string> settings)
        {
            using (_logger.Track("Applying field renamer."))
            {
                return Task.FromResult(ApplyInternal(flattened,
                    settings?.ToDictionary(k => k.Key.Prefix(), v => v.Value)));
            }
        }

        private static (bool, IDictionary<string, object>) ApplyInternal(IDictionary<string, object> flattened, IDictionary<string, string> maps = null)
        {
            var renamed = new Dictionary<string, object>();
            foreach (var (key, value) in maps.GetMatchingMaps(flattened).ToList())
            {
                if (flattened[key] == null || !(flattened[key] is { } o)) continue;
                renamed.Add(value.Prefix(), o);
                flattened.Remove(key);
            }

            flattened.ForEach(flat => renamed.Add(flat.Key, flat.Value));
            return (false, renamed);
        }
    }
}