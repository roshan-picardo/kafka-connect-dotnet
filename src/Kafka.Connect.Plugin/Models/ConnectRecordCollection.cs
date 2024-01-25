using System;
using System.Threading.Tasks;

namespace Kafka.Connect.Plugin.Models;

public interface IConnectRecordCollection
{
    void Setup(string connector, int taskId);
    void Clear();
    bool TrySubscribe();
    Task Consume();
    Task Process();
    Task Sink();
    void Commit();
    Task DeadLetter(Exception ex);
    void Record();
    Task NotifyEndOfPartition();
    void Cleanup();
    ConnectRecordBatch GetBatch();
}

