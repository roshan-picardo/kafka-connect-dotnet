using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Processors
{
    public class DateTimeTypeOverrider : Processor<IDictionary<string, string>>
    {
        private readonly ILogger<DateTimeTypeOverrider> _logger;

        public DateTimeTypeOverrider(ILogger<DateTimeTypeOverrider> logger, IConfigurationProvider configurationProvider) : base(configurationProvider)
        {
            _logger = logger;
        }
        
        protected override Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, IDictionary<string, string> settings)
        {
            using (_logger.Track("Applying datetime type overrider."))
            {
                return Task.FromResult(ApplyInternal(flattened,
                    settings?.ToDictionary(k => k.Key.Prefix(), v => v.Value)));
            }
        }

        private static (bool, IDictionary<string, object>) ApplyInternal(IDictionary<string, object> flattened, IDictionary<string, string> maps = null)
        {
            maps ??= new Dictionary<string, string>();
            foreach (var (key, value) in maps.GetMatchingMaps(flattened, true))
            {
                if (flattened[key] == null || flattened[key] is not string s) continue;
                if (!string.IsNullOrEmpty(value))
                {
                    if (DateTime.TryParseExact(s, value, CultureInfo.InvariantCulture, DateTimeStyles.None,
                        out var dateTime))
                    {
                        flattened[key] = dateTime;
                    }
                }
                else
                {
                    if (DateTime.TryParse(s, out var dateTime))
                    {
                        flattened[key] = dateTime;
                    }
                }
            }

            return (false, flattened);
        }
    }
}