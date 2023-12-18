using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Processors;

public interface IProcessor
{
    Task<(bool Skip, ConnectMessage<IDictionary<string, object>> Flattened)> Apply(string connector, ConnectMessage<IDictionary<string, object>> message);
}