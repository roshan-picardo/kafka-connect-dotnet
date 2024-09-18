using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Providers;

public class ConnectPluginFactory(
    IEnumerable<IProcessor> processors,
    IEnumerable<IMessageConverter> messageConverters,
    IEnumerable<IStrategySelector> strategySelectors,
    IEnumerable<IStrategy> queryStrategies,
    IConfigurationProvider configurationProvider)
    : IConnectPluginFactory
{
    public IProcessor GetProcessor(string name) => processors.SingleOrDefault(p => p.Is(name));

    public IMessageConverter GetMessageConverter(string name) => messageConverters.SingleOrDefault(p => p.Is(name));

    public IStrategy GetStrategy(string connector, IConnectRecord record)
    {
        var config = configurationProvider.GetPluginConfig(connector).Strategy;
        IStrategy strategy = null;

        if (config?.Name != null)
        {
            strategy =
                queryStrategies.SingleOrDefault(s => s.GetType().FullName == config.Name);
        }

        if (config?.Selector != null)
        {
            var selector =
                strategySelectors.SingleOrDefault(s => s.GetType().FullName == config.Selector);
            strategy = selector?.GetStrategy(record as ConnectRecord, config.Settings) ??
                       strategy;
        }

        return strategy ??
               throw new ConnectDataException("Strategy not defined.", new ArgumentException("Strategy not defined."));
    }
}