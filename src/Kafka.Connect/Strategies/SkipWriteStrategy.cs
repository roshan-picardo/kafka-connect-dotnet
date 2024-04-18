using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Strategies;

public class SkipWriteStrategy : IReadWriteStrategy
{
    public Task<(SinkStatus Status, IList<T> Models)> BuildModels<T>(string connector, IConnectRecord record)
    {
        return Task.FromResult<(SinkStatus, IList<T>)>((SinkStatus.Skipping, Array.Empty<T>()));
    }

    public Task<StrategyModel<T>> Build<T>(string connector, IConnectRecord record)
    {
        return Task.FromResult(new StrategyModel<T>
        {
            Status = SinkStatus.Skipping,
            Topic = record.Topic, 
            Partition = record.Partition, 
            Offset = record.Offset,
            Models = Array.Empty<T>()
        });
    }
}