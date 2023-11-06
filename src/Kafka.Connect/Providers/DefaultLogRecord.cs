using System.Linq;
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
            return record.Deserialized.Value is not { HasValues: true } ? null : attributes?.ToDictionary<string, string, object>(a => a, a => record.Deserialized.Value?[a]);
        }
    }
}