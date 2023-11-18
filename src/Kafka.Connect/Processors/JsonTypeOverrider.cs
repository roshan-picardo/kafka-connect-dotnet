using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Providers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Processors
{
    public class JsonTypeOverrider : Processor<IList<string>>
    {
        private readonly ILogger<JsonTypeOverrider> _logger;

        public JsonTypeOverrider(
            ILogger<JsonTypeOverrider> logger,
            IConfigurationProvider configurationProvider) : base(configurationProvider)
        {
            _logger = logger;
        }

        protected override Task<ConnectMessage<IDictionary<string, object>>> Apply(
            IList<string> settings,
            ConnectMessage<IDictionary<string, object>> message)
        {
            using (_logger.Track("Applying json type overrider."))
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

        private  IDictionary<string, object> ApplyInternal(IDictionary<string, object> flattened,
            IEnumerable<string> fields = null)
        {
            foreach (var key in fields.GetMatchingKeys(flattened).ToList())
            {
                JToken jToken = null;
                if (flattened[key] == null || flattened[key] is not string s) continue;
                try
                {
                    if (s.Trim().StartsWith("{") && s.Trim().EndsWith("}"))
                    {
                        jToken = JObject.Parse(s);
                    }
                    else if (s.Trim().StartsWith("[") && s.Trim().EndsWith("]"))
                    {
                        jToken = JArray.Parse(s);
                    }
                }
                catch (Exception ex)
                {
                    _logger.Warning($"Error while parsing JSON for key: {key}.", ex);
                }

                if (jToken == null) continue;
                flattened.Remove(key);
                foreach (var (k, v) in  jToken.ToJsonNode().ToDictionary())
                {
                    flattened.Add(jToken is JObject ? $"{key}.{k}" : $"{key}{k}", v);
                }
            }

            return flattened;
        }
    }
}