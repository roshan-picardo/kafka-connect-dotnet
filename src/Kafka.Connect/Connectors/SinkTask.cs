using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Serilog.Context;

namespace Kafka.Connect.Connectors;

public class SinkTask : ISinkTask
{
    private readonly ISinkExceptionHandler _sinkExceptionHandler;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IExecutionContext _executionContext;
    private readonly IConnectRecordCollection _connectRecordCollection;
    private readonly PauseTokenSource _pauseTokenSource;

    public SinkTask(
        ISinkExceptionHandler sinkExceptionHandler,
        IConfigurationProvider configurationProvider,
        IExecutionContext executionContext,
        IConnectRecordCollection connectRecordCollection)
    {
        _sinkExceptionHandler = sinkExceptionHandler;
        _configurationProvider = configurationProvider;
        _executionContext = executionContext;
        _connectRecordCollection = connectRecordCollection;
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
        
        using (LogContext.PushProperty("GroupId", _configurationProvider.GetGroupId(connector)))
        {
            var batchPollContext = _executionContext.GetOrSetBatchContext(connector, taskId, cts.Token);
            _connectRecordCollection.Setup(connector, taskId);
            if (!_connectRecordCollection.TrySubscribe())
            {
                IsStopped = true;
                return;
            }
            
            while (!cts.IsCancellationRequested)
            {
                await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);
                //TODO: lets approach this solution differently - need an Admin node to issue pause / resume over all workers.
                if (cts.IsCancellationRequested) break;

                batchPollContext.Reset(_executionContext.GetNextPollIndex());
                _connectRecordCollection.Clear();
                using (LogContext.PushProperty("Batch", batchPollContext.Iteration))
                {
                    try
                    {
                        await _connectRecordCollection.Consume();
                        await _connectRecordCollection.Process();
                        await _connectRecordCollection.Sink();
                        _connectRecordCollection.Commit();
                    }
                    catch (Exception ex)
                    {
                        if (_configurationProvider.IsErrorTolerated(connector))
                        {
                            await _connectRecordCollection.DeadLetter(ex);
                            _connectRecordCollection.Commit();
                        }
                        _sinkExceptionHandler.Handle(ex, Cancel);
                    }
                    finally
                    {   
                        _connectRecordCollection.Record();
                        await _connectRecordCollection.NotifyEndOfPartition();
                    }
                }
            }

            _connectRecordCollection.Cleanup();
        }

        IsStopped = true;
    }
}