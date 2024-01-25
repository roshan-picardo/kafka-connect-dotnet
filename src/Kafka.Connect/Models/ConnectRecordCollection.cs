using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Providers;
using Serilog.Context;
using Serilog.Core.Enrichers;
using IConfigurationProvider = Kafka.Connect.Providers.IConfigurationProvider;

namespace Kafka.Connect.Models;

public class ConnectRecordCollection : BlockingCollection<ConnectRecord>, IConnectRecordCollection
{
    private readonly Plugin.Logging.ILogger<ConnectRecordCollection> _logger;
    private readonly ISinkConsumer _sinkConsumer;
    private readonly IMessageHandler _messageHandler;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly ISinkHandlerProvider _sinkHandlerProvider;
    private readonly IPartitionHandler _partitionHandler;
    private readonly ISinkExceptionHandler _sinkExceptionHandler;
    private readonly IEnumerable<ILogRecord> _logRecords;
    private readonly IList<(string Topic, int Partition, long Offset)> _eofPartitions;
    private IConsumer<byte[], byte[]> _consumer;
    private string _connector;
    private int _taskId;

    public ConnectRecordCollection(
        Plugin.Logging.ILogger<ConnectRecordCollection> logger,
        ISinkConsumer sinkConsumer,
        IMessageHandler messageHandler,
        IConfigurationProvider configurationProvider,
        ISinkHandlerProvider sinkHandlerProvider,
        IPartitionHandler partitionHandler,
        ISinkExceptionHandler sinkExceptionHandler,
        IEnumerable<ILogRecord> logRecords)
    {
        _logger = logger;
        _sinkConsumer = sinkConsumer;
        _messageHandler = messageHandler;
        _configurationProvider = configurationProvider;
        _sinkHandlerProvider = sinkHandlerProvider;
        _partitionHandler = partitionHandler;
        _sinkExceptionHandler = sinkExceptionHandler;
        _logRecords = logRecords;
        _eofPartitions = new List<(string Topic, int Partition, long Offset)>();
    }

    public void Setup(string connector, int taskId)
    {
        _connector = connector;
        _taskId = taskId;
    }

    public void Clear()
    {
        using (_logger.Track("Clearing collection"))
        {
            while (Count > 0)
            {
                Take();
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

    public async Task Consume()
    {
        var batch = await _sinkConsumer.Consume(_consumer, _connector, _taskId);
        foreach (var record in batch)
        {
            if (record.IsPartitionEof)
            {
                _eofPartitions.Add((record.Topic, record.Partition, record.Offset));
            }
            else
            {
                Add(record);
            }
        }
    }

    public async Task Process()
    {
        if (Count <= 0) return;
        using (_logger.Track("Processing the batch."))
        {
            foreach (var topicBatch in GetByTopicPartition())
            {
                using (LogContext.Push(new PropertyEnricher(Constants.Topic, topicBatch.Topic),
                           new PropertyEnricher(Constants.Partition, topicBatch.Partition)))
                {
                    await ParallelEx.ForEachAsync(topicBatch.Batch.ToList(), _configurationProvider.GetDegreeOfParallelism(_connector), async record =>
                    {
                        if(record.Status == SinkStatus.Processed) return;
                        using (LogContext.PushProperty(Constants.Offset, record.Offset))
                        {
                            record.Status = SinkStatus.Processing;
                            var deserialized =
                                await _messageHandler.Deserialize(_connector, record.Topic, record.Serialized);
                            _logger.Document(deserialized);
                            (record.Skip, record.Deserialized) =
                                await _messageHandler.Process(_connector, record.Topic, deserialized);
                            record.Status = SinkStatus.Processed;
                        }
                    });
                }
            }
        }
    }

    public async Task Sink()
    {
        if (Count <= 0) return;
        using (_logger.Track("Sinking the batch."))
        {
            var sinkHandler = _sinkHandlerProvider.GetSinkHandler(_connector);
            if (sinkHandler == null)
            {
                _logger.Warning(
                    "Sink handler is not specified. Check if the handler is configured properly, and restart the connector.");
                ParallelEx.ForEach(this, record =>
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
                    await ParallelEx.ForEachAsync(batch.Batch.ToList(), _configurationProvider.GetDegreeOfParallelism(_connector), async record =>
                    {
                        if(record.Status is SinkStatus.Updated or SinkStatus.Deleted or SinkStatus.Inserted or SinkStatus.Skipped) return;
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
            ParallelEx.ForEach(this, record => record.UpdateStatus());
        }
    }

    public void Commit() => _partitionHandler.Commit(_consumer, GetCommitReadyOffsets());

    public Task DeadLetter(Exception ex) =>
        _sinkExceptionHandler.HandleDeadLetter(this.Select(r => r as SinkRecord).ToList(), ex, _connector);
   

    public void Record()
    {
        if (Count <= 0) return;
        var provider = _configurationProvider.GetLogEnhancer(_connector);
        var endTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var logRecord = _logRecords?.SingleOrDefault(l => l.GetType().FullName == provider);
        ParallelEx.ForEach(this, record =>
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
                    Timers = record.EndTiming(Count, endTime),
                    Attributes = attributes
                }, record.Exception);
            }
        });
    }

    public Task NotifyEndOfPartition() => _partitionHandler.NotifyEndOfPartition(_consumer, _connector, _taskId, _eofPartitions, GetCommitReadyOffsets());
    
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

    private IList<(string Topic, int Partition, IEnumerable<ConnectRecord> Batch)> GetByTopicPartition()
    {
        return (from record in this
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
        return (from record in this
            where record.IsCommitReady(isTolerated)
            select (record.Topic, record.Partition, record.Offset)).ToList();
    }
}