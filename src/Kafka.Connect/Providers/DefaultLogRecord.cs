using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Providers;

public class DefaultLogRecord(Kafka.Connect.Plugin.Providers.IConfigurationProvider configurationProvider)
    : ILogRecord
{
    public object Enrich(ConnectRecord record, string connector)
    {
        var attributes = configurationProvider.GetLogAttributes<string[]>(connector);
        var value = record.Deserialized.Value?.ToDictionary() ?? new Dictionary<string, object>();
        return value.Where(v => attributes.Contains(v.Key) && v.Value != null).ToDictionary().ToNested();
    }
}