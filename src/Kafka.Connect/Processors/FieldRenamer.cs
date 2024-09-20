using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Processors;

public class FieldRenamer(ILogger<FieldRenamer> logger, IConfigurationProvider configurationProvider)
    : Processor<IDictionary<string, string>>(configurationProvider)
{
    protected override Task<(bool Skip,  ConnectMessage<IDictionary<string, object>> Flattened)> Apply(IDictionary<string, string> settings, ConnectMessage<IDictionary<string, object>> message)
    {
        using (logger.Track("Applying field renamer."))
        {
            var processed = new ConnectMessage<IDictionary<string, object>>
            {
                Key = ApplyInternal(message.Key,
                    settings?.Where(s => s.Key.StartsWith("key")).ToDictionary(s => s.Key, s => s.Value)),
                Value = ApplyInternal(message.Value,
                    settings?.Where(s => !s.Key.StartsWith("key")).ToDictionary(s => s.Key, s => s.Value)),
            };
            return Task.FromResult((false, processed));
        }
    }

    private static IDictionary<string, object> ApplyInternal(IDictionary<string, object> flattened, IDictionary<string, string> maps = null)
    {
        var renamed = new Dictionary<string, object>();
        foreach (var (key, value) in maps.GetMatchingMaps(flattened).ToList())
        {
            if (flattened[key] == null || !(flattened[key] is { } o)) continue;
            renamed[value] = o;
            flattened.Remove(key);
        }

        flattened.ForEach(flat => renamed.Add(flat.Key, flat.Value));
        return renamed;
    }
}