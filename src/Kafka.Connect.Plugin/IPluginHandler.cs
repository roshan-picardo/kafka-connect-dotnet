using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Plugin;

public interface IPluginHandler
{
    Task Startup(string connector);
    Task<IList<ConnectRecord>> Get(string connector, int taskId, CommandRecord command);
    Task Put(IEnumerable<ConnectRecord> models, string connector, int taskId);

    IDictionary<string, Command> Commands(string connector);
    JsonNode NextCommand(CommandRecord command, List<ConnectRecord> records);
    
    bool Is(string connector, string plugin, string handler);
}

public abstract class PluginHandler(IConfigurationProvider configurationProvider) : IPluginHandler
{
    public abstract Task Startup(string connector);
    public abstract Task<IList<ConnectRecord>> Get(string connector, int taskId, CommandRecord command);
    public abstract Task Put(IEnumerable<ConnectRecord> models, string connector, int taskId);
    
    public abstract IDictionary<string, Command> Commands(string connector);
    public abstract JsonNode NextCommand(CommandRecord command, List<ConnectRecord> records);

    public bool Is(string connector, string plugin, string handler) =>
        plugin == configurationProvider.GetPluginName(connector) && this.Is(handler);
}
    
