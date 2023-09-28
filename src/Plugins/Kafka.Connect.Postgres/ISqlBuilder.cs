using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Postgres;

public interface ISqlBuilder
{
    Task<(SinkStatus, IEnumerable<string>)> BuildQuery(SinkRecord record, string table, string[] keys = null);
}