using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Plugin;

public abstract class SinkHandler : ISinkHandler
{
    private readonly ILogger<SinkHandler> _logger;
    private readonly IReadWriteStrategyProvider _readWriteStrategyProvider;
    private readonly IConfigurationProvider _configurationProvider;

    protected SinkHandler(ILogger<SinkHandler> logger, 
        IReadWriteStrategyProvider readWriteStrategyProvider,
        IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _readWriteStrategyProvider = readWriteStrategyProvider;
        _configurationProvider = configurationProvider;
    }
    
    public Task Startup(string connector) => Task.CompletedTask;

    public Task Cleanup(string connector) => Task.CompletedTask;

    public bool Is(string connector, string plugin, string handler) => plugin == _configurationProvider.GetPluginName(connector) && this.Is(handler);

    public abstract Task Put(IEnumerable<ConnectRecord> models, string connector, int taskId);

    protected IReadWriteStrategy GetReadWriteStrategy(string connector, IConnectRecord record) =>
        _readWriteStrategyProvider.GetSinkReadWriteStrategy(connector, record);
}