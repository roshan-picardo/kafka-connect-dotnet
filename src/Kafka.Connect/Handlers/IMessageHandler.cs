using System.Threading.Tasks;
using Kafka.Connect.Models;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Handlers
{
    public interface IMessageHandler
    {
        Task<(bool, JToken)> Process(ConnectRecord record, string connector);
    }
}