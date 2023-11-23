using System.Linq;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Providers
{
    public class DefaultLogRecord : ILogRecord
    {
        private readonly Kafka.Connect.Plugin.Providers.IConfigurationProvider _configurationProvider;

        public DefaultLogRecord(Kafka.Connect.Plugin.Providers.IConfigurationProvider configurationProvider)
        {
            _configurationProvider = configurationProvider;
        }
        public object Enrich(ConnectRecord record, string connector)
        {
            var attributes = _configurationProvider.GetLogAttributes<string[]>(connector);
            return record.Deserialized?.Value == null ? null : attributes?.ToDictionary(a => a, a => record.Deserialized.Value?[a]?.GetValue());
        }
    }
}