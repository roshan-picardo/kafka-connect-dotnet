using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Plugin.Providers;

public interface IReadWriteStrategyProvider
{
    IQueryStrategy GetSinkReadWriteStrategy(string connector, IConnectRecord record);
    IQueryStrategy GetSourceReadWriteStrategy(string connector, IConnectRecord record);
}