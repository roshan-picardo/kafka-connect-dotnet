using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Plugin.Providers;

public interface IConnectPluginFactory
{
    IProcessor GetProcessor(string name);
    IMessageConverter GetMessageConverter(string typeName);
    IStrategy GetStrategy(string connector, IConnectRecord record);
}