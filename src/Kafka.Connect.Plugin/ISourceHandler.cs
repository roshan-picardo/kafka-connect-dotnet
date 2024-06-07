using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin;

public interface ISourceHandler
{
    Task<IList<ConnectRecord>> Get(string connector, int taskId, CommandRecord command);
    bool Is(string connector, string plugin, string handler);
    IDictionary<string, Command> GetCommands(string connector);
    CommandRecord GetUpdatedCommand(CommandRecord command, IList<ConnectMessage<JsonNode>> records);
}