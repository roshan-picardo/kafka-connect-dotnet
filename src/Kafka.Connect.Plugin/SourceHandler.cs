using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Plugin;

public abstract class SourceHandler(
    IConfigurationProvider configurationProvider,
    IReadWriteStrategyProvider readWriteStrategyProvider)
    : ISourceHandler
{
    public abstract Task<IList<ConnectRecord>> Get(string connector, int taskId, CommandRecord command);

    public bool Is(string connector, string plugin, string handler) => plugin == configurationProvider.GetPluginName(connector) && this.Is(handler);

    public abstract IDictionary<string, Command> GetCommands(string connector);

    public abstract CommandRecord GetUpdatedCommand(
        CommandRecord command,
        IList<(SinkStatus Status, JsonNode Key)> records);

    protected IQueryStrategy GetReadWriteStrategy(string connector, IConnectRecord record) =>
        readWriteStrategyProvider.GetSourceReadWriteStrategy(connector, record);
}
