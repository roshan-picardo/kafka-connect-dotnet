using System.Threading.Tasks;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Handlers
{
    public interface IMessageHandler
    {
        Task<(bool, ConnectMessage<JToken, JToken>)> Process(ConnectRecord record, string connector);
    }
}