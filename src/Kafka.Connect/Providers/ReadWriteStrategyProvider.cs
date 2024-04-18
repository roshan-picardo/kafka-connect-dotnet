using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Strategies;

namespace Kafka.Connect.Providers;

public class ReadWriteStrategyProvider : IReadWriteStrategyProvider
{
    private readonly IEnumerable<IReadWriteStrategySelector> _writeStrategySelectors;
    private readonly IEnumerable<IReadWriteStrategy> _readWriteStrategies;
    private readonly IConfigurationProvider _configurationProvider;

    public ReadWriteStrategyProvider(IEnumerable<IReadWriteStrategySelector> writeStrategySelectors, 
        IEnumerable<IReadWriteStrategy> readWriteStrategies,
        IConfigurationProvider configurationProvider)
    {
        _writeStrategySelectors = writeStrategySelectors;
        _readWriteStrategies = readWriteStrategies;
        _configurationProvider = configurationProvider;
    }

    public IReadWriteStrategy GetSinkReadWriteStrategy(string connector, IConnectRecord record) =>
        GetReadWriteStrategy(_configurationProvider.GetSinkConfig(connector).Strategy, record);


    public IReadWriteStrategy GetSourceReadWriteStrategy(string connector, IConnectRecord record) =>
        GetReadWriteStrategy(_configurationProvider.GetSourceConfig(connector).Strategy, record);
    
    private IReadWriteStrategy GetReadWriteStrategy(StrategyConfig strategyConfig, IConnectRecord record)
    {
        IReadWriteStrategy strategy = null;

        if (strategyConfig?.Name != null)
        {
            strategy =
                _readWriteStrategies.SingleOrDefault(s => s.GetType().FullName == strategyConfig.Name);
        }

        if (strategyConfig?.Selector?.Name != null)
        {
            var selector =
                _writeStrategySelectors.SingleOrDefault(s => s.GetType().FullName == strategyConfig.Selector.Name);
            strategy = selector?.GetReadWriteStrategy(record, strategyConfig.Selector.Overrides) ??
                       strategy;
        }
        return strategy ?? throw new ConnectDataException("Strategy not defined.", new ArgumentException("Strategy not defined."));
    }
}