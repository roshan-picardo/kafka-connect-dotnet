using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers;

public interface IConnectRecordCollection
{
    void Setup(string connector, int taskId);
    void Clear(string batchId = null);
    bool TrySubscribe();
    Task Consume();
    Task Process(string batchId = null);
    Task Sink();
    void Commit();
    Task DeadLetter(Exception ex);
    void Record(string batchId = null);
    Task NotifyEndOfPartition();
    void Cleanup();
    ConnectRecordBatch GetBatch();
    bool TryPublisher();
    Task<(int TimeOut, IList<CommandRecord> Commands)> GetCommands(string connector);
    Task Source(CommandRecord command);
    Task Produce(string batchId = null);
    Task UpdateCommand(CommandRecord command);
    void Commit(IList<CommandRecord> commands);
}

