using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Processors
{
    public class JsonTypeOverrider : Processor<IList<string>>
    {
        private readonly IRecordFlattener _recordFlattener;
        private readonly ILogger<JsonTypeOverrider> _logger;

        public JsonTypeOverrider(IRecordFlattener recordFlattener, ILogger<JsonTypeOverrider> logger, IOptions<IList<ConnectorConfig<IList<string>>>> options, IOptions<ConnectorConfig<IList<string>>> shared) 
            : base(options, shared)
        {
            _recordFlattener = recordFlattener;
            _logger = logger;
        }

        [OperationLog("Applying json type overrider.")]
        protected override Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, IList<string> settings)
        {
            return Task.FromResult(ApplyInternal(flattened, settings?.Select(s=> s.Prefix())));
        }

        private (bool, IDictionary<string, object>) ApplyInternal(IDictionary<string, object> flattened,
            IEnumerable<string> fields = null)
        {
            foreach (var key in fields.GetMatchingKeys(flattened).ToList())
            {
                JToken jObject = null;
                if (!flattened.ContainsKey(key) || flattened[key] == null || flattened[key] is not string s) continue;
                try
                {
                    if (s.Trim().StartsWith("{") && s.Trim().EndsWith("}"))
                    {
                        jObject = JObject.Parse(s);
                    }
                    else if (s.Trim().StartsWith("[") && s.Trim().EndsWith("]"))
                    {
                        jObject = JArray.Parse(s);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "{@Log}", new {Message = "Error while parsing JSON data."});
                }

                if (jObject == null) continue;
                flattened.Remove(key);
                foreach (var (k, v) in  _recordFlattener.Flatten(jObject))
                {
                    flattened.Add(jObject is JObject ? $"{key}.{k}" : $"{key}{k}", v);
                }
            }

            return (false, flattened);
        }
    }
}