using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers;

public interface IConnectRecordCollection
{
    void Setup(ConnectorType connectorType, string connector, int taskId);
    void Clear(string batchId = null);
    void ClearAll();
    bool TrySubscribe();
    Task Consume(CancellationToken token);
    Task Process(string batchId = null);
    Task Sink();
    void Commit();
    Task DeadLetter(Exception ex);
    void Record(string batchId = null);
    Task NotifyEndOfPartition();
    void Cleanup();
    ConnectRecordBatch GetBatch();
    bool TryPublisher();
    Task<(int TimeOut, IList<CommandRecord> Commands)> GetCommands();
    Task Source(CommandRecord command);
    Task Produce(string batchId = null);
    Task UpdateCommand(CommandRecord command);
    void Commit(IList<CommandRecord> commands);
    Task Configure(string batchId, bool refresh);
    void UpdateTo(SinkStatus status, string batchId = null);
    void StartTiming();
    void EndTiming();
}

