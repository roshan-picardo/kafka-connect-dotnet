using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using IConfigurationProvider = Kafka.Connect.Providers.IConfigurationProvider;

namespace Kafka.Connect.Handlers;

public class ConnectRecordCollection(
    ILogger<ConnectRecordCollection> logger,
    IMessageHandler messageHandler,
    IConfigurationProvider configurationProvider,
    IEnumerable<IPluginHandler> pluginHandlers,
    IExecutionContext executionContext,
    IConfigurationChangeHandler configurationChangeHandler,
    IConnectorClient connectClient,
    IEnumerable<ILogRecord> logRecords)
    : IConnectRecordCollection
{
    private readonly IDictionary<(string Topic, int Partition), long> _eofPartitions = new Dictionary<(string Topic, int Partition), long>();
    private string _connector;
    private int _taskId;
    private readonly BlockingCollection<ConnectRecord> _sinkConnectRecords = new();
    private readonly ConcurrentDictionary<string, BlockingCollection<ConnectRecord>> _sourceConnectRecords = new();
    private readonly Stopwatch _stopWatch = new();

    public async Task Setup(ConnectorType connectorType, string connector, int taskId)
    {
        _connector = connector;
        _taskId = taskId;
        if (connectorType is ConnectorType.Sink or ConnectorType.Source)
        {
            await GetPluginHandler(connector).Startup(connector);
        }
    }

    public void Clear(string batchId = null)
    {
        using (logger.Track("Clearing collection"))
        {
            var batch = GetConnectRecords(batchId);
            while (batch.Count > 0)
            {
                batch.Take();
            }
        }
    }

    public void ClearAll()
    {
        Clear();
        _sourceConnectRecords.Clear();
    }

    public bool TrySubscribe() => connectClient.TryBuildSubscriber(_connector, _taskId);


    public bool TryPublisher() => connectClient.TryBuildPublisher(_connector);

    public async Task Consume(CancellationToken token)
    {
        var batch = await connectClient.Consume(_connector, _taskId, token);
        foreach (var record in batch)
        {
            if (record.IsPartitionEof)
            {
                if(_eofPartitions.ContainsKey((record.Topic, record.Partition)))
                {
                    _eofPartitions[(record.Topic, record.Partition)] = record.Offset;
                }
                else
                {
                    _eofPartitions.Add((record.Topic, record.Partition), record.Offset);
                }
            }
            else
            {
                _sinkConnectRecords.Add(record, token);
            }
        }
    }

    public Task Produce(string batchId = null) => connectClient.Produce(_connector, GetConnectRecords(batchId).Select(t => t as IConnectRecord).ToList());

    public async Task<JsonNode> UpdateCommand(CommandRecord command)
    {
        var batch = GetConnectRecords(command.Id.ToString());
        if (batch is { Count: > 0 })
        {
            var pluginHandler = GetPluginHandler(_connector);
            if (pluginHandler != null)
            {
                command.Command = pluginHandler.NextCommand(command, batch.ToList());
            }
        }

        var message = await messageHandler.Serialize(command.Connector, command.Topic, new ConnectMessage<JsonNode>
        {
            Key = command.Id.ToString(),
            Value = System.Text.Json.JsonSerializer.SerializeToNode(command)
        });

        await connectClient.Produce(new TopicPartition(command.Topic, command.Partition),
            new Message<byte[], byte[]> { Key = message.Key, Value = message.Value });

        return command.Command;
    }

    public void Commit(IList<CommandRecord> commands)
    {
        foreach (var command in commands)
        {
            executionContext.SetPartitionEof(_connector, _taskId, command.Topic, command.Partition, false);
        }

        var offsets = _eofPartitions.Where(eof => commands.Any(c => c.Partition == eof.Key.Partition) && eof.Value > 0)
            .Select(eof => (eof.Key.Topic, eof.Key.Partition, eof.Value - 1));
        connectClient.Commit(offsets.ToList());
    }

    public async Task Configure(string batchId, bool refresh)
    {
        var batch = await configurationChangeHandler.Refresh(_sinkConnectRecords, refresh);
        _sourceConnectRecords.AddOrUpdate(batchId, batch, (_, _) => batch);
    }

    public void UpdateTo(Status status, string batchId = null)
    {
        var sourceRecords = GetConnectRecords(batchId).ToList();
        var latestRecords = GetConnectRecords(null).GroupBy(r => r.GetKey<string>())
            .Select(g => g.Aggregate((max, cur) => (max == null || cur.Offset > max.Offset) ? cur : max)).ToList();
        foreach (var record in latestRecords.Where(record => sourceRecords.Exists(s => s.GetKey<string>() == record.GetKey<string>())))
        {
            record.Status = status;
        }
    }

    public void UpdateTo(Status status, string topic, int partition, long offset, Exception ex = null)
    {
        var record = GetRecordByTopicPartitionOffset(topic, partition, offset);

        if (record == null) return;
        
        record.Status = status;
        if (ex != null)
        {
            record.Exception = ex;
        }
    }

    public int Count(string batchId = null) => GetConnectRecords(batchId).Count;

    public async Task Process(string batchId = null)
    {
        if (GetConnectRecords(batchId).Count <= 0) return;
        using (logger.Track("Processing the batch."))
        {
            await GetConnectRecords(batchId).ForEachAsync(configurationProvider.GetParallelRetryOptions(_connector),
                async icr =>
                {
                    if (icr is not ConnectRecord { Processing: true } record) return;
                    record.Status = Status.Processing;
                    (bool Skip, ConnectMessage<JsonNode> Message) processed = new (false, new ConnectMessage<JsonNode>());   
                    switch (record)
                    {
                        case SinkRecord:
                            using (ConnectLog.TopicPartitionOffset(record.Topic, record.Partition, record.Offset))
                            {
                                var deserialized =
                                    await messageHandler.Deserialize(_connector, record.Topic, record.Serialized);
                                logger.Document(deserialized);
                                record.Raw = deserialized;
                                processed = await messageHandler.Process(_connector, record.Topic, deserialized);
                            }

                            break;
                        case SourceRecord or ConfigRecord:
                            logger.Document(record.Deserialized);
                            record.Raw = record.Deserialized;
                            processed = await messageHandler.Process(_connector, record.Topic, record.Deserialized);
                            record.Serialized = await messageHandler.Serialize(_connector, record.Topic, processed.Message);
                            break;
                    }

                    record.Status = processed.Skip ? Status.Skipped : Status.Processed;
                    record.Deserialized = processed.Message;
                });
        }
    }

    public async Task Sink()
    {
        if (_sinkConnectRecords.Count <= 0) return;
        using (logger.Track("Sinking the batch."))
        {
            var pluginHandler = GetPluginHandler(_connector);
            if (pluginHandler == null)
            {
                throw new ConnectDataException(
                    "Sink handler is not specified. Check if the handler is configured properly, and restart the connector.",
                    new ArgumentException(nameof(pluginHandler)));
            }

            await pluginHandler.Put(_sinkConnectRecords.ToList(), _connector, _taskId);
            _sinkConnectRecords.ForEach(record => record.UpdateStatus());
        }
    }

    public void Commit() => connectClient.Commit(GetCommitReadyOffsets());

    public Task DeadLetter(string batchId = null) => connectClient.SendToDeadLetter(GetConnectRecords(batchId), _connector, _taskId);

    public void Record(string batchId = null) =>
        Record(GetConnectRecords(batchId).ToList(), GetConnectRecords(batchId).Count);

    public void Record(CommandRecord command)
    {
        var records = GetConnectRecords(command.Id.ToString()).Select(r => r).ToList();
        var record = _sinkConnectRecords.FirstOrDefault(r => r.IsOf(command.Topic, command.Partition, command.Offset));
        if (record != null)
        {
            record.Status = command.Status;
            record.Exception = command.Exception;
            records.Add(record);
        }

        Record(records, GetConnectRecords(command.Id.ToString()).Count);
    }

    private void Record(List<ConnectRecord> records, int count)
    {
        if (records.Count <= 0) return;
        var provider = configurationProvider.GetLogEnhancer(_connector);
        var endTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var logRecord = logRecords?.SingleOrDefault(l => l.GetType().FullName == provider);
        records.ForEach(record =>
        {
            using (ConnectLog.TopicPartitionOffset(record.Topic, record.Partition, record.Offset))
            {
                object attributes = null;
                try
                {
                    attributes = logRecord?.Enrich(record, _connector);
                }
                catch (Exception)
                {
                    // ignored
                }

                record.UpdateStatus(true);
                logger.Record(new
                {
                    record.Status,
                    Timers = record.EndTiming(count, endTime),
                    Attributes = attributes
                }, record.Exception);
            }
        });
    }

    public Task NotifyEndOfPartition() => connectClient.NotifyEndOfPartition(_connector, _taskId,
        _eofPartitions.Select(eof => (eof.Key.Topic, eof.Key.Partition, eof.Value)).ToList(), GetCommitReadyOffsets());

    public void Cleanup()
    {
        Clear();
        _sourceConnectRecords.Clear();
        connectClient.Close();
    }

    public async Task<IList<CommandRecord>> GetCommands()
    {
        using (logger.Track("Sourcing the poll commands.."))
        {
            var batch = configurationProvider.GetBatchConfig(_connector);
            var commandTopic = configurationProvider.GetTopic(TopicType.Command);
            var pluginHandler = GetPluginHandler(_connector);
            if (pluginHandler != null)
            {
                var commands = pluginHandler.Commands(_connector);
                var partitions = executionContext.GetAssignedPartitions(_connector, _taskId)
                    .SingleOrDefault(p => p.Key == commandTopic).Value;
                var pollCommands = commands.Select(command => new CommandRecord
                {
                    Name = command.Key,
                    Connector = _connector,
                    Command = command.Value.ToJson(),
                    Topic = commandTopic,
                    Partition = -1,
                    BatchSize = batch.Size, 
                }).Where(command => partitions.Contains(GetCommandPartition(command))).ToList();

                foreach (var topicBatch in GetByTopicPartition())
                {
                    foreach (var record in topicBatch.Batch.OrderByDescending(r => r.Offset))
                    {
                        using (ConnectLog.TopicPartitionOffset(record.Topic, record.Partition, record.Offset))
                        {
                            record.Deserialized =
                                await messageHandler.Deserialize(_connector, record.Topic, record.Serialized);
                            logger.Document(record.Deserialized);
                            var commandRecord = record.GetValue<CommandRecord>();
                            var pollCommand =
                                pollCommands.Find(command => command.Id == commandRecord.Id);
                            if (pollCommand == null) continue;
                            pollCommand.Partition = record.Partition;
                            pollCommand.Offset = record.Offset;
                            if (pollCommand.GetVersion() == commandRecord.GetVersion())
                            {
                                pollCommand.Command = commandRecord.Command;
                            }

                            pollCommand.Topic = commandRecord.Topic;
                            break;
                        }
                    }
                }

                foreach (var command in pollCommands.Where(command => command.Partition == -1))
                {
                    command.Partition = GetCommandPartition(command);
                    command.Status = Status.Consumed;
                }
                return pollCommands;
            }
            return Array.Empty<CommandRecord>();
        }
    } 
    
    public async Task Source(CommandRecord command)
    {
        using (logger.Track("Sourcing the batch."))
        {
            var batch = _sourceConnectRecords.AddOrUpdate(command.Id.ToString(), new BlockingCollection<ConnectRecord>(),
                (_, _) => new BlockingCollection<ConnectRecord>());
            var pluginHandler = GetPluginHandler(_connector);
            if (pluginHandler != null)
            {
                var records = await pluginHandler.Get(_connector, _taskId, command);
                if (records != null)
                {
                    foreach (var record in records)
                    {
                        batch.Add(record.Clone<SourceRecord>());
                    }
                }
            }
        }
    }

    public void StartTiming() => _stopWatch.Restart();
    public void EndTiming() => _stopWatch.Stop();

    private List<(string Topic, int Partition, IEnumerable<ConnectRecord> Batch)> GetByTopicPartition(string batchId = null)
    {
        return (from record in GetConnectRecords(batchId)
            group record by new { record.Topic, record.Partition }
            into tp
            select (tp.Key.Topic, tp.Key.Partition, StopByStatus(tp.Select(r => r)))).ToList();

        IEnumerable<ConnectRecord> StopByStatus(IEnumerable<ConnectRecord> records)
        {
            var isErrorTolerated = configurationProvider.IsErrorTolerated(_connector);
            foreach (var record in records)
            {
                if (!isErrorTolerated && record.Status == Status.Failed) break;
                yield return record;
            }
        }
    }

    private List<(string Topic, int Partition, long Offset)> GetCommitReadyOffsets() => _sinkConnectRecords
        .TakeWhile(record => record.Status != Status.Aborted)
        .Select(record => (record.Topic, record.Partition, record.Offset)).ToList();

    private BlockingCollection<ConnectRecord> GetConnectRecords(string batchId) => string.IsNullOrWhiteSpace(batchId)
        ? _sinkConnectRecords
        : _sourceConnectRecords.GetOrAdd(batchId, new BlockingCollection<ConnectRecord>());

    private static int GetCommandPartition(CommandRecord command) => (command.Id.GetHashCode() & 0x7FFFFFFF) % 50;
    
    private IPluginHandler GetPluginHandler(string connector)
    {
        var config = configurationProvider.GetPluginConfig(connector);
        var pluginHandler = pluginHandlers.SingleOrDefault(s => s.Is(connector, config.Name, config.Handler));
        logger.Trace("Selected plugin handler.", new { config.Name, Handler = pluginHandler?.GetType().FullName });
        return pluginHandler;
    }
    
    private ConnectRecord GetRecordByTopicPartitionOffset(string topic, int partition, long offset) => 
        _sinkConnectRecords.SingleOrDefault(r =>
        r.Topic == topic && r.Partition == partition && r.Offset == offset) ?? _sourceConnectRecords
        .SelectMany(s => s.Value)
        .SingleOrDefault(r => r.Topic == topic && r.Partition == partition && r.Offset == offset);
}
