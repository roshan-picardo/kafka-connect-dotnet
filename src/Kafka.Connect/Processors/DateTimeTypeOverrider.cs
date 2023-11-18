using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
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

        protected override Task<ConnectMessage<IDictionary<string, object>>> Apply(
            IDictionary<string, string> settings,
            ConnectMessage<IDictionary<string, object>> message)
        {
            using (_logger.Track("Applying datetime type overrider."))
            {
                var processed = new ConnectMessage<IDictionary<string, object>>
                {
                    Skip = false,
                    Key = ApplyInternal(message.Key,
                        settings?.Where(s => s.Key.StartsWith("key")).ToDictionary(s => s.Key, s => s.Value)),
                    Value = ApplyInternal(message.Value,
                        settings?.Where(s => !s.Key.StartsWith("key")).ToDictionary(s => s.Key, s => s.Value)),
                };
                return Task.FromResult(processed);
            }
        }

        private static IDictionary<string, object> ApplyInternal(IDictionary<string, object> flattened, IDictionary<string, string> maps = null)
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

            return flattened;
        }
    }
}