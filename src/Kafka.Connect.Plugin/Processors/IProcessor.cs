using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Processors;

public interface IProcessor
{
    Task<ConnectMessage<IDictionary<string, object>>> Apply(string connector, ConnectMessage<IDictionary<string, object>> message);
}