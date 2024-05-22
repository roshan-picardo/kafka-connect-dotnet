using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Providers;
using Kafka.Connect.Utilities;
using Serilog.Context;
using Serilog.Core.Enrichers;
using IConfigurationProvider = Kafka.Connect.Providers.IConfigurationProvider;

namespace Kafka.Connect.Handlers;

public class ConnectRecordCollection(
    Plugin.Logging.ILogger<ConnectRecordCollection> logger,
    ISinkConsumer sinkConsumer,
    ISourceProducer sourceProducer,
    IMessageHandler messageHandler,
    IConfigurationProvider configurationProvider,
    IConnectHandlerProvider sinkHandlerProvider,
    IPartitionHandler partitionHandler,
    ISinkExceptionHandler sinkExceptionHandler,
    IExecutionContext executionContext,
    IConfigurationChangeHandler configurationChangeHandler,
    IEnumerable<ILogRecord> logRecords)
    : IConnectRecordCollection
{
    private readonly IDictionary<(string Topic, int Partition), long> _eofPartitions = new Dictionary<(string Topic, int Partition), long>();
    private IConsumer<byte[], byte[]> _consumer;
    private IProducer<byte[], byte[]> _producer;
    private string _connector;
    private int _taskId;
    private readonly BlockingCollection<ConnectRecord> _sinkConnectRecords = new();
    private readonly ConcurrentDictionary<string, BlockingCollection<ConnectRecord>> _sourceConnectRecords = new();

    public void Setup(ConnectorType connectorType, string connector, int taskId)
    {
        _connector = connector;
        _taskId = taskId;
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

    public bool TrySubscribe()
    {
        _consumer = sinkConsumer.Subscribe(_connector, _taskId);
        if (_consumer != null)
        {
            return true;
        }
        logger.Warning("Failed to create the consumer, exiting from the sink task.");
        return false;
    }
    
    public bool TryPublisher()
    {
        _producer = sourceProducer.GetProducer(_connector, _taskId);
        if (_producer != null)
        {
            return true;
        }
        logger.Warning("Failed to create the publisher, exiting from the source task.");
        return false;
    }

    public async Task Consume()
    {
        var batch = await sinkConsumer.Consume(_consumer, _connector, _taskId);
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
                _sinkConnectRecords.Add(record);
            }
        }
    }

    public async Task Produce(string batchId = null)
    {
        using (logger.Track("Publishing the batch."))
        {
            foreach (var record in GetConnectRecords(batchId))
            {
                using (LogContext.PushProperty("Topic", record.Topic))
                {
                    if (record.Skip)
                    {
                        record.Status = SinkStatus.Skipped;
                        continue;
                    }

                    record.Status = SinkStatus.Publishing;

                    var delivered = await _producer.ProduceAsync(record.Topic,
                        new Message<byte[], byte[]>
                        {
                            Key = record.Serialized.Key,
                            Value = record.Serialized.Value,
                            Headers = record.Serialized.Headers?.ToMessageHeaders()
                        });
                    record.Published(delivered.Topic, delivered.Partition, delivered.Offset);
                }
            }
        }
    }

    public async Task UpdateCommand(CommandRecord command)
    {
        var batch = GetConnectRecords(command.Id);
        if (batch != null && batch.Any())
        {
            var sourceHandler = sinkHandlerProvider.GetSourceHandler(_connector);
            if (sourceHandler != null)
            {
                command = sourceHandler.GetUpdatedCommand(command,
                    batch.Select(r => (r.Status, r.GetKey<JsonNode>())).ToList());
            }
        }

        var message = await messageHandler.Serialize(command.Connector, command.Topic, new ConnectMessage<JsonNode>
        {
            Key = null,
            Value = System.Text.Json.JsonSerializer.SerializeToNode(command)
        });

        await _producer.ProduceAsync(new TopicPartition(command.Topic, command.Partition),
            new Message<byte[], byte[]> { Key = message.Key, Value = message.Value });
    }

    public void Commit(IList<CommandRecord> commands)
    {
        foreach (var command in commands)
        {
            executionContext.SetPartitionEof(_connector, _taskId, command.Topic, command.Partition, false);
        }

        var offsets = _eofPartitions.Where(eof => eof.Value > 0)
            .Select(eof => (eof.Key.Topic, eof.Key.Partition, eof.Value - 1));
        partitionHandler.Commit(_consumer, offsets.ToList());
    }

    public async Task Configure(string batchId, bool refresh)
    {
        var batch = await configurationChangeHandler.Refresh(_sinkConnectRecords, refresh);
        _sourceConnectRecords.AddOrUpdate(batchId, batch, (_, _) => batch);
    }

    public void UpdateTo(SinkStatus status, string batchId = null)
    {
        var sourceRecords = GetConnectRecords(batchId).ToList();
        var latestRecords = GetConnectRecords(null).GroupBy(r => r.GetKey<string>())
            .Select(g => g.Aggregate((max, cur) => (max == null || cur.Offset > max.Offset) ? cur : max)).ToList();
        foreach (var record in latestRecords.Where(record => sourceRecords.Exists(s => s.GetKey<string>() == record.GetKey<string>())))
        {
            record.Status = status;
        }
    }

    public async Task Process(string batchId = null)
    {
        if (GetConnectRecords(batchId).Count <= 0) return;
        using (logger.Track("Processing the batch."))
        {
            foreach (var topicBatch in GetByTopicPartition(batchId))
            {
                await topicBatch.Batch.ForEachAsync(configurationProvider.GetDegreeOfParallelism(_connector), async cr =>
                    {
                        if (cr is not ConnectRecord record || record.Status == SinkStatus.Processed) return;
                        record.Status = SinkStatus.Processing;

                        switch (record)
                        {
                            case SinkRecord:
                                using (LogContext.Push(new PropertyEnricher(Constants.Topic, record.Topic),
                                           new PropertyEnricher(Constants.Partition, record.Partition),
                                           new PropertyEnricher(Constants.Offset, record.Offset)))
                                {
                                    var deserialized =
                                        await messageHandler.Deserialize(_connector, record.Topic, record.Serialized);
                                    logger.Document(deserialized);
                                    (record.Skip, record.Deserialized) =
                                        await messageHandler.Process(_connector, record.Topic, deserialized);
                                }
                                break;
                            case SourceRecord or ConfigRecord:
                                logger.Document(record.Deserialized);
                                (record.Skip, record.Deserialized) =
                                    await messageHandler.Process(_connector, record.Topic, record.Deserialized);
                                record.Serialized =
                                    await messageHandler.Serialize(_connector, record.Topic, record.Deserialized);
                                break;
                        }

                        record.Status = SinkStatus.Processed;
                    });
            }
        }
    }

    public async Task Sink()
    {
        if (_sinkConnectRecords.Count <= 0) return;
        using (logger.Track("Sinking the batch."))
        {
            var sinkHandler = sinkHandlerProvider.GetSinkHandler(_connector);
            if (sinkHandler == null)
            {
                logger.Warning(
                    "Sink handler is not specified. Check if the handler is configured properly, and restart the connector.");
                ParallelEx.ForEach(_sinkConnectRecords, record =>
                {
                    // TODO: do not do skip, rather error out here!!
                    record.Status = SinkStatus.Skipped;
                    record.Skip = true;
                });
                return;
            }

            foreach (var batch in GetByTopicPartition())
            {
                await sinkHandler.Put(batch.Batch, _connector, _taskId);
                ParallelEx.ForEach(batch.Batch, record => record.UpdateStatus());
            }
        }
    }

    public void Commit() => partitionHandler.Commit(_consumer, GetCommitReadyOffsets());

    public Task DeadLetter(Exception ex) =>
        sinkExceptionHandler.HandleDeadLetter(_sinkConnectRecords.Select(r => r as SinkRecord).ToList(), ex, _connector);

    public void Record(string batchId = null)
    {
        if (GetConnectRecords(batchId).Count <= 0) return;
        var provider = configurationProvider.GetLogEnhancer(_connector);
        var endTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var logRecord = logRecords?.SingleOrDefault(l => l.GetType().FullName == provider);
        ParallelEx.ForEach(GetConnectRecords(batchId), record =>
        {
            using (LogContext.Push(new PropertyEnricher("Topic", record.Topic),
                       new PropertyEnricher("Partition", record.Partition),
                       new PropertyEnricher("Offset", record.Offset)))
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
                    Timers = record.EndTiming(GetConnectRecords(batchId).Count, endTime),
                    Attributes = attributes
                }, record.Exception);
            }
        });
    }

    public Task NotifyEndOfPartition() => partitionHandler.NotifyEndOfPartition(_consumer, _connector, _taskId,
        _eofPartitions.Select(eof => (eof.Key.Topic, eof.Key.Partition, eof.Value)).ToList(), GetCommitReadyOffsets());

    public void Cleanup()
    {
        Clear();
        _sourceConnectRecords.Clear();
        _consumer?.Close();
        _consumer?.Dispose();
        _producer?.Dispose();
    }

    public ConnectRecordBatch GetBatch()
    {
        return new ConnectRecordBatch("internal");
    }

    public async Task<(int TimeOut, IList<CommandRecord> Commands)> GetCommands(string connector)
    {
        
        //1. No messages available
            // build poll commands with a Map for all commands and assign partitions
        using (logger.Track("Sourcing the poll commands.."))
        {
            var sourceHandler = sinkHandlerProvider.GetSourceHandler(_connector);
            if (sourceHandler != null)
            {
                var config = configurationProvider.GetSourceConfig(_connector);
                var commands = sourceHandler.GetCommands(_connector);
                var pollCommands = commands.Select(command => new CommandRecord
                {
                    Name = command.Key,
                    Connector = connector,
                    Partition = -1,
                    Command = command.Value.ToJson(),
                    Topic = config.Topic,
                    BatchSize = config.BatchSize, 
                }).ToList();

                foreach (var topicBatch in GetByTopicPartition())
                {
                    using (LogContext.Push(new PropertyEnricher(Constants.Topic, topicBatch.Topic),
                               new PropertyEnricher(Constants.Partition, topicBatch.Partition)))
                    {
                        foreach (var record in topicBatch.Batch.OrderByDescending(r => r.Offset))
                        {
                            using (LogContext.PushProperty(Constants.Offset, record.Offset))
                            {
                                record.Deserialized =
                                    await messageHandler.Deserialize(connector, record.Topic, record.Serialized);
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
                }

                foreach (var command in pollCommands.Where(command => command.Partition == -1))
                {
                    command.Partition = command.Id.GetHashCode() % 50;
                }

                return (config.TimeOutInMs, pollCommands);
            }

            return (0, null); // TODO: this needs a fix
        }
    } 
    public async Task Source(CommandRecord command)
    {
        using (logger.Track("Sourcing the batch."))
        {
            var batch = _sourceConnectRecords.AddOrUpdate(command.Id, new BlockingCollection<ConnectRecord>(),
                (_, _) => new BlockingCollection<ConnectRecord>());
            var sourceHandler = sinkHandlerProvider.GetSourceHandler(_connector);
            if (sourceHandler != null)
            {
                foreach (var record in await sourceHandler.Get(_connector, _taskId, command))
                {
                    batch.Add(new SourceRecord(record.Topic, record.Deserialized.Key ?? new JsonObject(), record.Deserialized.Value));
                }
            }
        }
    }

    private IList<(string Topic, int Partition, IEnumerable<ConnectRecord> Batch)> GetByTopicPartition(string batchId = null)
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
                if (!isErrorTolerated && record.Status == SinkStatus.Failed) break;
                yield return record;
            }
        }
    }
    
    private IList<(string Topic, int Partition, long Offset)> GetCommitReadyOffsets()
    {
        var isTolerated = configurationProvider.IsErrorTolerated(_connector);
        return (from record in _sinkConnectRecords
            where record.IsCommitReady(isTolerated)
            select (record.Topic, record.Partition, record.Offset)).ToList();
    }

    private BlockingCollection<ConnectRecord> GetConnectRecords(string batchId) => string.IsNullOrWhiteSpace(batchId)
        ? _sinkConnectRecords
        : _sourceConnectRecords.GetOrAdd(batchId, new BlockingCollection<ConnectRecord>());
}