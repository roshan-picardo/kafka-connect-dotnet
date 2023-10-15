using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Strategies;

public interface IWriteStrategy
{
    Task<(SinkStatus Status, IList<T> Models)> BuildModels<T>(string connector, ConnectRecord record);
}