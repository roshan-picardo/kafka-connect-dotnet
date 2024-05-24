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
    private readonly IEnumerable<IStrategySelector> _writeStrategySelectors;
    private readonly IEnumerable<IQueryStrategy> _readWriteStrategies;
    private readonly IConfigurationProvider _configurationProvider;

    public ReadWriteStrategyProvider(IEnumerable<IStrategySelector> writeStrategySelectors, 
        IEnumerable<IQueryStrategy> readWriteStrategies,
        IConfigurationProvider configurationProvider)
    {
        _writeStrategySelectors = writeStrategySelectors;
        _readWriteStrategies = readWriteStrategies;
        _configurationProvider = configurationProvider;
    }

    public IQueryStrategy GetSinkReadWriteStrategy(string connector, IConnectRecord record) =>
        GetReadWriteStrategy(_configurationProvider.GetSinkConfig(connector).Strategy, record);


    public IQueryStrategy GetSourceReadWriteStrategy(string connector, IConnectRecord record) =>
        GetReadWriteStrategy(_configurationProvider.GetSourceConfig(connector).Strategy, record);
    
    private IQueryStrategy GetReadWriteStrategy(StrategyConfig strategyConfig, IConnectRecord record)
    {
        IQueryStrategy strategy = null;

        if (strategyConfig?.Name != null)
        {
            strategy =
                _readWriteStrategies.SingleOrDefault(s => s.GetType().FullName == strategyConfig.Name);
        }

        if (strategyConfig?.Selector?.Name != null)
        {
            var selector =
                _writeStrategySelectors.SingleOrDefault(s => s.GetType().FullName == strategyConfig.Selector.Name);
            strategy = selector?.GetQueryStrategy(record, strategyConfig.Selector.Overrides) ??
                       strategy;
        }
        return strategy ?? throw new ConnectDataException("Strategy not defined.", new ArgumentException("Strategy not defined."));
    }
}