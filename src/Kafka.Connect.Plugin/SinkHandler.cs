using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Plugin;

public abstract class SinkHandler(
    ILogger<SinkHandler> logger,
    IReadWriteStrategyProvider readWriteStrategyProvider,
    IConfigurationProvider configurationProvider)
    : ISinkHandler
{
    private readonly ILogger<SinkHandler> _logger = logger;

    public Task Startup(string connector) => Task.CompletedTask;

    public Task Cleanup(string connector) => Task.CompletedTask;

    public bool Is(string connector, string plugin, string handler) => plugin == configurationProvider.GetPluginName(connector) && this.Is(handler);

    public abstract Task Put(IEnumerable<ConnectRecord> models, string connector, int taskId);

    protected IQueryStrategy GetReadWriteStrategy(string connector, IConnectRecord record) =>
        readWriteStrategyProvider.GetSinkReadWriteStrategy(connector, record);
}