using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers;

public interface IConnectRecordCollection
{
    Task Setup(ConnectorType connectorType, string connector, int taskId);
    Task Purge(ConnectorType connectorType, string connector, int taskId);
    void Clear(string batchId = null);
    void ClearAll();
    bool TrySubscribe();
    Task Consume(CancellationToken token);
    Task Process(string batchId = null);
    Task Sink();
    void Commit();
    Task DeadLetter(string batchId = null);
    void Record(string batchId = null);
    void Record(CommandRecord command);
    Task NotifyEndOfPartition();
    void Cleanup();
    bool TryPublisher();
    Task<IList<CommandRecord>> GetCommands();
    Task Source(CommandRecord command);
    Task Produce(string batchId = null);
    Task<JsonNode> UpdateCommand(CommandRecord command);
    void Commit(IList<CommandRecord> commands);
    Task Configure(string batchId, bool refresh);
    void UpdateTo(Status status, string batchId = null);
    int Count(string batchId = null);
    void StartTiming();
    void EndTiming();
}
