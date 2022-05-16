using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Handlers
{
    public interface IMessageHandler
    {
        Task<(bool, JToken)> Process(SinkRecord record, string connector);
    }
}