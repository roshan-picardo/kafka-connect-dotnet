using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Providers;

public interface ILogRecord
{
    object Enrich(SinkRecord record, string connector);
}