using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Plugin.Providers;

public interface IWriteStrategyProvider
{
    IWriteStrategy GetWriteStrategy(string connector, SinkRecord record);
}