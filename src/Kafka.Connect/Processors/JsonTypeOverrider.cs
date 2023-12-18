using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Processors;

public class JsonTypeOverrider : Processor<IList<string>>
{
    private readonly ILogger<JsonTypeOverrider> _logger;

    public JsonTypeOverrider(
        ILogger<JsonTypeOverrider> logger,
        IConfigurationProvider configurationProvider) : base(configurationProvider)
    {
        _logger = logger;
    }

    protected override Task<(bool Skip,  ConnectMessage<IDictionary<string, object>> Flattened)> Apply(
        IList<string> settings,
        ConnectMessage<IDictionary<string, object>> message)
    {
        using (_logger.Track("Applying json type overrider."))
        {
            var processed = new ConnectMessage<IDictionary<string, object>>
            {
                Key = ApplyInternal(message.Key, settings?.Where(s => s.StartsWith("key"))),
                Value = ApplyInternal(message.Value, settings?.Where(s => !s.StartsWith("key"))),
            };
            return Task.FromResult((false, processed));
        }
    }

    private  IDictionary<string, object> ApplyInternal(IDictionary<string, object> flattened, IEnumerable<string> fields = null)
    {
        foreach (var key in fields.GetMatchingKeys(flattened).ToList())
        {
            JsonNode jn = null;
            if (flattened[key] == null || flattened[key] is not string s) continue;
            try
            {
                jn = JsonNode.Parse(s);
            }
            catch (Exception ex)
            {
                _logger.Warning($"Error while parsing JSON for key: {key}.", ex);
            }

            if (jn == null) continue;
            flattened.Remove(key);
            foreach (var (k, v) in  jn.ToDictionary())
            {
                flattened.Add(jn is JsonObject ? $"{key}.{k}" : $"{key}{k}", v);
            }
        }

        return flattened;
    }
}