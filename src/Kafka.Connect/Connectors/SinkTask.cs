using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Utilities;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using Serilog.Core.Enrichers;

[assembly:InternalsVisibleTo("Kafka.Connect.Tests")]
namespace Kafka.Connect.Connectors
{
    public class SinkTask : ISinkTask
    {
        private readonly ILogger<SinkTask> _logger;
        private IConsumer<byte[], byte[]> _consumer;
        private readonly ISinkConsumer _sinkConsumer;
        private readonly ISinkProcessor _sinkProcessor;
        private readonly IPartitionHandler _partitionHandler;
        private readonly ISinkExceptionHandler _sinkExceptionHandler;
        private readonly IRetriableHandler _retriableHandler;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IExecutionContext _executionContext;
        private readonly Func<string, IConsumerBuilder> _consumerBuilderFactory;

        public SinkTask(ILogger<SinkTask> logger,  ISinkConsumer sinkConsumer, ISinkProcessor sinkProcessor,
            IPartitionHandler partitionHandler, ISinkExceptionHandler sinkExceptionHandler, IRetriableHandler retriableHandler, IConfigurationProvider configurationProvider, IExecutionContext executionContext, Func<string, IConsumerBuilder> consumerBuilderFactory)
        {
            _logger = logger;
            _sinkConsumer = sinkConsumer;
            _sinkProcessor = sinkProcessor;
            _partitionHandler = partitionHandler;
            _sinkExceptionHandler = sinkExceptionHandler;
            _retriableHandler = retriableHandler;
            _configurationProvider = configurationProvider;
            _executionContext = executionContext;
            _consumerBuilderFactory = consumerBuilderFactory;
        }

        public async Task Execute(string connector, int taskId, CancellationToken cancellationToken)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            void Cancel()
            {
                if (!_configurationProvider.IsErrorTolerated(connector))
                {
                    cts.Cancel();
                }
            }
            
            _executionContext.Add(connector, taskId);
            _consumerBuilderFactory(connector).AttachPartitionChangeEvents(connector, taskId);
            _consumer = _logger.Timed("Subscribing to the topics.").Execute(() => _sinkConsumer.Subscribe(connector));
            if (_consumer == null)
            {
                _logger.LogWarning("{@Log}", new {Message = "Failed to create the consumer, exiting from the sink task."});
                return;
            }
            
            using (LogContext.Push(new PropertyEnricher("GroupId", _configurationProvider.GetGroupId(connector)),
                new PropertyEnricher("Consumer", _consumer.Name?.Replace(connector, ""))))
            {
                var batchPollContext = _executionContext.GetOrSetBatchContext(connector, taskId, cts.Token);

                while (!cts.IsCancellationRequested)
                {
                    batchPollContext.Reset(_executionContext.GetNextPollIndex());
                    SinkRecordBatch batch = null;
                    using (LogContext.PushProperty("Batch", batchPollContext.Iteration))
                    {
                        try
                        {
                            batch = await _logger.Timed("Invoking consume.").Execute(() => _sinkConsumer.Consume(_consumer, connector, taskId));
                            batch = await _retriableHandler.Retry(b => ProcessAndSinkInternal(connector, b, Cancel), batch, connector);
                        }
                        catch (Exception ex)
                        {
                            _logger.Timed("Handle processing errors.").Execute(() => _sinkExceptionHandler.Handle(ex, Cancel));
                        }
                        finally
                        {
                            if (!cts.IsCancellationRequested)
                            {
                                await CommitAndLog(batch, connector, taskId);
                            }
                            _executionContext.AddToCount(batch?.Count ?? 0);
                        }
                    }
                }

                Cleanup();
                _executionContext.Stop(connector, taskId);
            }
        }

        private async Task<SinkRecordBatch> ProcessAndSinkInternal(string connector, SinkRecordBatch batch, Action cancelToken)
        {
            if (batch == null || !batch.Any()) return batch;
            try
            {
                await _logger.Timed("Processing the batch.").Execute(() => _sinkProcessor.Process(batch, connector));
                await _logger.Timed("Sinking the batch.").Execute(() => _sinkProcessor.Sink(batch, connector));
                batch.MarkAllCommitReady();
            }
            catch (Exception ex)
            {
                if (_configurationProvider.IsErrorTolerated(connector) && batch.IsLastAttempt)
                {
                    _logger.Timed("Handle processing errors.").Execute(() => _sinkExceptionHandler.Handle(ex,cancelToken));
                    await _logger.Timed("Handover to dead-letter.").Execute(() => _sinkExceptionHandler.HandleDeadLetter(batch, ex, connector));
                    batch.MarkAllCommitReady(true);
                }
                else
                {
                    throw;
                }
            }

            return batch;
        }

        private async Task CommitAndLog(SinkRecordBatch batch,  string connector, int taskId)
        {
            if (batch != null && batch.Any())
            {
                if (batch.GetCommitReadyOffsets().Any())
                {
                    _logger.Timed("Committing offsets.").Execute(() => _partitionHandler.CommitOffsets(batch, _consumer));
                }

                foreach (var record in batch)
                {
                    using (LogContext.Push(new PropertyEnricher("Topic", record.Topic),
                        new PropertyEnricher("Partition", record.Partition),
                        new PropertyEnricher("Offset", record.Offset)))
                    {
                        record.AddLog("Timing", record.Headers.EndTiming(batch.Count));
                        _logger.LogInformation("{@Record}", record.GetLogs());
                    }
                } 
                await _logger.Timed("Notify end of the partition.").Execute(async () =>  await _partitionHandler.NotifyEndOfPartition(batch, connector, taskId));
            }
            _logger.LogDebug("{@Log}",
                new
                {
                    Records = batch?.Count ?? 0,
                    Duration = _executionContext.GetOrSetBatchContext(connector, taskId).Timer.EndTiming(),
                    Message = "Finished processing the batch.",
                    Status = batch?.GetBatchStatus()
                });
        }
        
        private void Cleanup()
        {
            if (_consumer == null) return;
            _consumer.Close();
            _consumer.Dispose();
        }
    }
}