using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Connectors;

public interface IExecutionContext
{
    void AssignPartitions(string connector, int task, IEnumerable<TopicPartition> partitions);
    void RevokePartitions(string connector, int task, IEnumerable<TopicPartition> partitions);
    IDictionary<string, List<int>> GetAssignedPartitions(string connector, int task);
    dynamic GetStatus(string connector = null, int task = 0);
    void AddToCount(int records);
    dynamic GetFullDetails();
    void Shutdown();
    CancellationTokenSource GetToken();
    bool IsStopped { get; }
    void Initialize(string name, IWorker worker);
    void Initialize(string name, ILeader leader);
    void Initialize(string name, IConnector connector);
    void Initialize(string connector, int taskId, ITask task);
    void Pause(string connector = null, int task = 0);
    void Resume(string connector = null, int task = 0);
    Task Restart(int delay, string connector = null, int task = 0);
    IConnector GetConnector(string connector);
    ITask GetSinkTask(string connector, int task);
    Task<bool> Retry(string connector = null, int task = 0);
    void SetPartitionEof(string connector, int task, string topic, int partition, bool eof);
    bool AllPartitionEof(string connector, int task);
    void UpdateCommands(string connector, int task, IEnumerable<CommandRecord> tasks);
}