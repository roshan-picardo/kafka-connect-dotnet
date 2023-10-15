using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Strategies;

namespace Kafka.Connect.Providers;

public class WriteStrategyProvider : IWriteStrategyProvider
{
    private readonly IEnumerable<IWriteStrategySelector> _writeStrategySelectors;
    private readonly IEnumerable<IWriteStrategy> _writeStrategies;
    private readonly IConfigurationProvider _configurationProvider;

    public WriteStrategyProvider(IEnumerable<IWriteStrategySelector> writeStrategySelectors, 
        IEnumerable<IWriteStrategy> writeStrategies, 
        IConfigurationProvider configurationProvider)
    {
        _writeStrategySelectors = writeStrategySelectors;
        _writeStrategies = writeStrategies;
        _configurationProvider = configurationProvider;
    }

    public IWriteStrategy GetWriteStrategy(string connector, Plugin.Models.ConnectRecord record)
    {
        var strategyConfig = _configurationProvider.GetSinkConfig(connector);
        IWriteStrategy strategy = null;
        if (strategyConfig?.Strategy.SkipOnFailure ?? false)
        {
            strategy = _writeStrategies.SingleOrDefault(s => s is SkipWriteStrategy);
        }

        if (strategyConfig?.Strategy?.Name != null)
        {
            strategy =
                _writeStrategies.SingleOrDefault(s => s.GetType().FullName == strategyConfig.Strategy.Name) ?? strategy;
        }

        if (strategyConfig?.Strategy?.Selector?.Name != null)
        {
            var selector =
                _writeStrategySelectors.SingleOrDefault(s => s.GetType().FullName == strategyConfig.Strategy.Selector.Name);
            strategy = selector?.GetWriteStrategy(record, strategyConfig.Strategy.Selector.Overrides) ??
                       strategy;
        }
        return strategy ?? throw new ConnectDataException("Strategy not defined.", new ArgumentException("Strategy not defined."));
    }
}