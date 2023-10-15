using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Providers;

public interface ILogRecord
{
    object Enrich(ConnectRecord record, string connector);
}