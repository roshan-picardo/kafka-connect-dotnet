using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Models;

namespace Kafka.Connect.Connectors;

public interface IExecutionContext
{
    void AssignPartitions(string connector, int task, IEnumerable<TopicPartition> partitions);
    void RevokePartitions(string connector, int task, IEnumerable<TopicPartition> partitions);
    dynamic GetStatus(string connector = null, int task = 0);
    int GetNextPollIndex();
    void AddToCount(int records);
    dynamic GetFullDetails();
    void Shutdown();
    CancellationTokenSource GetToken();
    bool IsStopped { get; }
    BatchPollContext GetOrSetBatchContext(string connector, int taskId, CancellationToken token = default);
    void Initialize(string name, IWorker worker);
    void Initialize(string name, IConnector connector);
    void Initialize(string connector, int taskId, ISinkTask task);
    void Pause(string connector = null, int task = 0);
    void Resume(string connector = null, int task = 0);
    Task Restart(int delay, string connector = null, int task = 0);
    IConnector GetConnector(string connector);
    ISinkTask GetSinkTask(string connector, int task);
    Task<bool> Retry(string connector = null, int task = 0);
}