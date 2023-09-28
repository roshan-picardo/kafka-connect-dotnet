using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Strategies;

public class SkipWriteStrategy : IWriteStrategy
{
    public Task<(SinkStatus Status, IList<T> Models)> BuildModels<T>(string connector, SinkRecord record)
    {
        return Task.FromResult<(SinkStatus, IList<T>)>((SinkStatus.Skipping, Array.Empty<T>()));
    }
}