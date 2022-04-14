using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Microsoft.Extensions.Options;

namespace Kafka.Connect.Processors
{
    public class DateTimeTypeOverrider : Processor<IDictionary<string, string>>
    {
        public DateTimeTypeOverrider(IOptions<List<ConnectorConfig<IDictionary<string, string>>>> options, IOptions<ConnectorConfig<IDictionary<string, string>>> shared) : base(options, shared)
        {
        }

        protected override Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, IDictionary<string, string> settings)
        {
            return Task.FromResult(ApplyInternal(flattened,
                settings?.ToDictionary(k => ProcessorHelper.PrefixValue(k.Key), v => v.Value)));
        }

        private static (bool, IDictionary<string, object>) ApplyInternal(IDictionary<string, object> flattened, IDictionary<string, string> maps = null)
        {
            throw new ConnectRetriableException(ErrorCode.Unknown, new DataException("hahahaha"));
            maps ??= new Dictionary<string, string>();
            foreach (var (key, value) in ProcessorHelper.GetMaps(flattened, maps, true))
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