using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using Kafka.Connect.Models;

namespace Kafka.Connect.Connectors
{
    public interface IExecutionContext
    {
        void Name(string worker);
        void Add(string connector = null, int task = 0);
        T GetOrAdd<T>(string connector = null, int task = 0);
        void Pause(string connector = null, int task = 0);
        void Stop(string connector = null, int task = 0);
        void Start(string connector = null, int task = 0);
        void Clear(string connector = null);
        void AssignPartitions(string connector, int task, IEnumerable<TopicPartition> partitions);
        void RevokePartitions(string connector, int task, IEnumerable<TopicPartition> partitions);
        dynamic GetStatus(string connector = null, int task = 0);
        int GetNextPollIndex();
        void AddToCount(int records);
        dynamic GetFullDetails();

        BatchPollContext GetOrSetBatchContext(string connector, int taskId, CancellationToken token = default);
    }
}