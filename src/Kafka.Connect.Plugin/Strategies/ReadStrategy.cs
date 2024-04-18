using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Strategies;

public abstract class ReadStrategy<T> : IWriteStrategy
{
    public async Task<(SinkStatus Status, IList<TType> Models)> BuildModels<TType>(string connector, IConnectRecord record)
    {
        var response = await BuildModels(connector, record as CommandRecord);
        return (response.Status, response.Models.Cast<TType>().ToList());
    }

    protected abstract Task<(SinkStatus Status, IList<T> Models)> BuildModels(string connector, CommandRecord record);
}