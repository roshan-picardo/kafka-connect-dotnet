using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Plugin.Providers;

public interface IReadWriteStrategyProvider
{
    IReadWriteStrategy GetSinkReadWriteStrategy(string connector, IConnectRecord record);
    IReadWriteStrategy GetSourceReadWriteStrategy(string connector, IConnectRecord record);
}