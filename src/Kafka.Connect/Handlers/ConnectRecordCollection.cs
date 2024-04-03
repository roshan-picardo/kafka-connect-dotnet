using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
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

public class ConnectRecordCollection : IConnectRecordCollection
{
    private readonly Plugin.Logging.ILogger<ConnectRecordCollection> _logger;
    private readonly ISinkConsumer _sinkConsumer;
    private readonly ISourceProducer _sourceProducer;
    private readonly IMessageHandler _messageHandler;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly ISinkHandlerProvider _sinkHandlerProvider;
    private readonly IPartitionHandler _partitionHandler;
    private readonly ISinkExceptionHandler _sinkExceptionHandler;
    private readonly IExecutionContext _executionContext;

    private readonly IEnumerable<ILogRecord> _logRecords;
    private readonly IDictionary<(string Topic, int Partition), long> _eofPartitions;
    private IConsumer<byte[], byte[]> _consumer;
    private IProducer<byte[], byte[]> _producer;
    private string _connector;
    private int _taskId;
    private readonly BlockingCollection<ConnectRecord> _sinkConnectRecords;
    private readonly ConcurrentDictionary<string, BlockingCollection<ConnectRecord>> _sourceConnectRecords;

    public ConnectRecordCollection(
        Plugin.Logging.ILogger<ConnectRecordCollection> logger,
        ISinkConsumer sinkConsumer,
        ISourceProducer sourceProducer,
        IMessageHandler messageHandler,
        IConfigurationProvider configurationProvider,
        ISinkHandlerProvider sinkHandlerProvider,
        IPartitionHandler partitionHandler,
        ISinkExceptionHandler sinkExceptionHandler,
        IExecutionContext executionContext,
        IEnumerable<ILogRecord> logRecords)
    {
        _logger = logger;
        _sinkConsumer = sinkConsumer;
        _sourceProducer = sourceProducer;
        _messageHandler = messageHandler;
        _configurationProvider = configurationProvider;
        _sinkHandlerProvider = sinkHandlerProvider;
        _partitionHandler = partitionHandler;
        _sinkExceptionHandler = sinkExceptionHandler;
        _executionContext = executionContext;
        _logRecords = logRecords;
        _eofPartitions = new Dictionary<(string Topic, int Partition), long>();
        _sinkConnectRecords = new BlockingCollection<ConnectRecord>();
        _sourceConnectRecords = new ConcurrentDictionary<string, BlockingCollection<ConnectRecord>>();
    }

    public void Setup(string connector, int taskId)
    {
        _connector = connector;
        _taskId = taskId;
    }

    public void Clear(string batchId = null)
    {
        using (_logger.Track("Clearing collection"))
        {
            var batch = GetConnectRecords(batchId);
            while (batch.Count > 0)
            {
                batch.Take();
            }
        }
    }

    public bool TrySubscribe()
    {
        _consumer = _sinkConsumer.Subscribe(_connector, _taskId);
        if (_consumer != null)
        {
            return true;
        }
        _logger.Warning("Failed to create the consumer, exiting from the sink task.");
        return false;
    }
    
    public bool TryPublisher()
    {
        _producer = _sourceProducer.GetProducer(_connector, _taskId);
        if (_producer != null)
        {
            return true;
        }
        _logger.Warning("Failed to create the publisher, exiting from the source task.");
        return false;
    }

    public async Task Consume()
    {
        var batch = await _sinkConsumer.Consume(_consumer, _connector, _taskId);
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

    public async Task Produce()
    {
        using (_logger.Track("Publishing the batch."))
        {
            foreach (var record in _sinkConnectRecords)
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
            var maxTimestamp = batch.Cast<SourceRecord>()
                .Where(r => r.Status is SinkStatus.Skipped or SinkStatus.Published)
                .Select(r => r.Timestamp)
                .DefaultIfEmpty()
                .Max();
            var maxUniqueId = batch.Cast<SourceRecord>()
                .Where(r => r.Timestamp == maxTimestamp)
                .Select(r => r.UniqueId)
                .DefaultIfEmpty()
                .Max();

            command.Command.Timestamp = maxTimestamp;
            command.Command.UniqueId = maxUniqueId;
        }

        var message = await _messageHandler.Serialize(command.Connector, command.Topic, new ConnectMessage<JsonNode>
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
            _executionContext.SetPartitionEof(_connector, _taskId, command.Topic, command.Partition, false);
        }

        var offsets = _eofPartitions.Where(eof => eof.Value > 0)
            .Select(eof => (eof.Key.Topic, eof.Key.Partition, eof.Value - 1));
        _partitionHandler.Commit(_consumer, offsets.ToList());
    }

    public async Task Process(string batchId = null)
    {
        if (GetConnectRecords(batchId).Count <= 0) return;
        using (_logger.Track("Processing the batch."))
        {
            foreach (var topicBatch in GetByTopicPartition(batchId))
            {
                await topicBatch.Batch.ForEachAsync(_configurationProvider.GetDegreeOfParallelism(_connector), async cr =>
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
                                        await _messageHandler.Deserialize(_connector, record.Topic, record.Serialized);
                                    _logger.Document(deserialized);
                                    (record.Skip, record.Deserialized) =
                                        await _messageHandler.Process(_connector, record.Topic, deserialized);
                                }
                                break;
                            case SourceRecord:
                                _logger.Document(record.Deserialized);
                                (record.Skip, record.Deserialized) =
                                    await _messageHandler.Process(_connector, record.Topic, record.Deserialized);
                                record.Serialized =
                                    await _messageHandler.Serialize(_connector, record.Topic, record.Deserialized);
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
        using (_logger.Track("Sinking the batch."))
        {
            var sinkHandler = _sinkHandlerProvider.GetSinkHandler(_connector);
            if (sinkHandler == null)
            {
                _logger.Warning(
                    "Sink handler is not specified. Check if the handler is configured properly, and restart the connector.");
                ParallelEx.ForEach(_sinkConnectRecords, record =>
                {
                    record.Status = SinkStatus.Skipped;
                    record.Skip = true;
                });
                return;
            }

            var sinkBatch = new BlockingCollection<ConnectRecordModel>();
            foreach (var batch in GetByTopicPartition())
            {
                using (LogContext.Push(new PropertyEnricher("topic", batch.Topic),
                           new PropertyEnricher("partition", batch.Partition)))
                {
                    await batch.Batch.ForEachAsync(_configurationProvider.GetDegreeOfParallelism(_connector), async cr =>
                    {
                        if (cr is not ConnectRecord record ||
                            record.Status is SinkStatus.Updated or SinkStatus.Deleted or SinkStatus.Inserted or SinkStatus.Skipped)
                            return;
                        using (LogContext.Push(new PropertyEnricher("offset", record.Offset)))
                        {
                            if (!record.Skip)
                            {
                                sinkBatch.Add(await sinkHandler.BuildModels(record, _connector));
                            }
                            else
                            {
                                record.Status = SinkStatus.Skipping;
                            }
                        }
                    });
                }
            }

            await sinkHandler.Put(sinkBatch, _connector, _taskId);
            ParallelEx.ForEach(_sinkConnectRecords, record => record.UpdateStatus());
        }
    }

    public void Commit() => _partitionHandler.Commit(_consumer, GetCommitReadyOffsets());

    public Task DeadLetter(Exception ex) =>
        _sinkExceptionHandler.HandleDeadLetter(_sinkConnectRecords.Select(r => r as SinkRecord).ToList(), ex, _connector);

    public void Record(string batchId = null)
    {
        if (GetConnectRecords(batchId).Count <= 0) return;
        var provider = _configurationProvider.GetLogEnhancer(_connector);
        var endTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var logRecord = _logRecords?.SingleOrDefault(l => l.GetType().FullName == provider);
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
                _logger.Record(new
                {
                    record.Status,
                    Timers = record.EndTiming(GetConnectRecords(batchId).Count, endTime),
                    Attributes = attributes
                }, record.Exception);
            }
        });
    }

    public Task NotifyEndOfPartition() => _partitionHandler.NotifyEndOfPartition(_consumer, _connector, _taskId,
        _eofPartitions.Select(eof => (eof.Key.Topic, eof.Key.Partition, eof.Value)).ToList(), GetCommitReadyOffsets());
    
    public void Cleanup()
    {
        Clear();
        if (_consumer == null) return;
        _consumer.Close();
        _consumer.Dispose();
    }

    public ConnectRecordBatch GetBatch()
    {
        return new ConnectRecordBatch("internal");
    }

    public async Task<(int TimeOut, IList<CommandRecord> Commands)> GetCommands(string connector)
    {
        using (_logger.Track("Processing the poll commands."))
        {
            var config = _configurationProvider.GetSourceConfig(connector);
            var pollCommands = config.Commands.Select(command => new CommandRecord
            {
                Name = command.Key,
                Connector = connector,
                Partition = -1,
                Command = command.Value,
                Topic = config.Topic,
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
                                await _messageHandler.Deserialize(connector, record.Topic, record.Serialized);
                            _logger.Document(record.Deserialized);
                            var commandRecord = record.GetValue<CommandRecord>();
                            var pollCommand =
                                pollCommands.Find(command => command.Id == commandRecord.Id);
                            if (pollCommand == null) continue;
                            pollCommand.Partition = record.Partition;
                            pollCommand.Offset = record.Offset;
                            pollCommand.Command = commandRecord.Command;
                            pollCommand.Topic = commandRecord.Topic;
                            break;
                        }
                    }
                }
            }

            if (pollCommands.Any(command => command.Partition == -1))
            {
                foreach (var (command, index) in pollCommands.OrderByDescending(command => command.Partition)
                             .Select((command, i) => (command, i)))
                {
                    if (command.Partition == -1)
                    {
                        command.Partition = index;
                    }
                }
            }

            return (config.TimeOutInMs, pollCommands);
        }
    }

    public Task Source(CommandRecord command)
    {
        using (_logger.Track("Sourcing the batch."))
        {
            var batch = _sourceConnectRecords.AddOrUpdate(command.Id, new BlockingCollection<ConnectRecord>(),
                (_, _) => new BlockingCollection<ConnectRecord>());
            // make sure to set the timestamp and UniqueId column value...
            return Task.CompletedTask;
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
            var isErrorTolerated = _configurationProvider.IsErrorTolerated(_connector);
            foreach (var record in records)
            {
                if (!isErrorTolerated && record.Status == SinkStatus.Failed) break;
                yield return record;
            }
        }
    }
    
    private IList<(string Topic, int Partition, long Offset)> GetCommitReadyOffsets()
    {
        var isTolerated = _configurationProvider.IsErrorTolerated(_connector);
        return (from record in _sinkConnectRecords
            where record.IsCommitReady(isTolerated)
            select (record.Topic, record.Partition, record.Offset)).ToList();
    }

    private BlockingCollection<ConnectRecord> GetConnectRecords(string batchId) => string.IsNullOrWhiteSpace(batchId)
        ? _sinkConnectRecords
        : _sourceConnectRecords.GetOrAdd(batchId, new BlockingCollection<ConnectRecord>());
}