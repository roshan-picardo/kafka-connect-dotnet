using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Kafka.Connect.Utilities;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Connectors;

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
    private readonly ITokenHandler _tokenHandler;
    private readonly PauseTokenSource _pauseTokenSource;

    public SinkTask(ILogger<SinkTask> logger, ISinkConsumer sinkConsumer, ISinkProcessor sinkProcessor,
        IPartitionHandler partitionHandler, ISinkExceptionHandler sinkExceptionHandler,
        IRetriableHandler retriableHandler, IConfigurationProvider configurationProvider,
        IExecutionContext executionContext, ITokenHandler tokenHandler)
    {
        _logger = logger;
        _sinkConsumer = sinkConsumer;
        _sinkProcessor = sinkProcessor;
        _partitionHandler = partitionHandler;
        _sinkExceptionHandler = sinkExceptionHandler;
        _retriableHandler = retriableHandler;
        _configurationProvider = configurationProvider;
        _executionContext = executionContext;
        _tokenHandler = tokenHandler;
        _pauseTokenSource = PauseTokenSource.New();
    }

    public bool IsPaused => false;
    public bool IsStopped { get; private set; }

    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {

        void Cancel()
        {
            if (!_configurationProvider.IsErrorTolerated(connector))
            {
                cts.Cancel();
            }
        }

        _executionContext.Initialize(connector, taskId, this);

        _consumer = _sinkConsumer.Subscribe(connector, taskId);
        if (_consumer == null)
        {
            _logger.Warning("Failed to create the consumer, exiting from the sink task.");
            IsStopped = true;
            return;
        }

        using (LogContext.Push(new PropertyEnricher("GroupId", _configurationProvider.GetGroupId(connector)),
                   new PropertyEnricher("Consumer", _consumer.Name?.Replace(connector, ""))))
        {
            var batchPollContext = _executionContext.GetOrSetBatchContext(connector, taskId, cts.Token);

            while (!cts.IsCancellationRequested)
            {
                _tokenHandler.DoNothing();
                await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);
                //TODO: lets approach this solution differently - need an Admin node to issue pause / resume over all workers.
                if (cts.IsCancellationRequested) break;

                batchPollContext.Reset(_executionContext.GetNextPollIndex());
                SinkRecordBatch batch = null;
                using (LogContext.PushProperty("Batch", batchPollContext.Iteration))
                {
                    try
                    {
                        batch = await _sinkConsumer.Consume(_consumer, connector, taskId);
                        batch = await _retriableHandler.Retry(b => ProcessAndSinkInternal(connector, taskId, b, Cancel),
                            batch, connector);
                    }
                    catch (Exception ex)
                    {
                        _sinkExceptionHandler.Handle(ex, Cancel);
                    }
                    finally
                    {
                        _partitionHandler.CommitOffsets(batch, _consumer);
                        _logger.Record(batch, _configurationProvider.GetLogEnhancer(connector), connector);
                        await _partitionHandler.NotifyEndOfPartition(batch, connector, taskId);
                        // if (!cts.IsCancellationRequested)
                        // {
                        //     await CommitAndLog(batch, connector, taskId);
                        // }
                        _executionContext.AddToCount(batch?.Count ?? 0);
                        
                        _logger.Debug("Finished processing the batch.",
                            new
                            {
                                Records = batch?.Count ?? 0,
                                Duration = _executionContext.GetOrSetBatchContext(connector, taskId).Timer.EndTiming(),
                                Stats = batch?.GetBatchStatus()
                            });
                    }
                }
            }

            Cleanup();
            //if(cts.IsCancellationRequested) continue;
            // do the retries!!
        }

        IsStopped = true;
    }

    private async Task<SinkRecordBatch> ProcessAndSinkInternal(string connector, int taskId, SinkRecordBatch batch, Action cancelToken)
    {
        if (batch == null || !batch.Any()) return batch;
        try
        {
            await _sinkProcessor.Process(batch, connector);
            await _sinkProcessor.Sink(batch, connector, taskId);
            batch.MarkAllCommitReady();
        }
        catch (Exception ex)
        {
            if (_configurationProvider.IsErrorTolerated(connector) && batch.IsLastAttempt)
            {
                _sinkExceptionHandler.Handle(ex, cancelToken);
                await _sinkExceptionHandler.HandleDeadLetter(batch, ex, connector);
                batch.MarkAllCommitReady(true);
            }
            else
            {
                throw;
            }
        }

        return batch;
    }

    private void Cleanup()
    {
        if (_consumer == null) return;
        _consumer.Close();
        _consumer.Dispose();
    }
}