using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface IMessageHandler
    {
        Task<ConnectMessage<JsonNode>> Process(
            string connector,
            string topic,
            ConnectMessage<IDictionary<string, object>> flattened);
    }
}