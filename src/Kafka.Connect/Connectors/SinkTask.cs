using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Handlers;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Serilog.Context;

namespace Kafka.Connect.Connectors;

public class SinkTask : ISinkTask
{
    private readonly ISinkExceptionHandler _sinkExceptionHandler;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IExecutionContext _executionContext;
    private readonly IConnectRecordCollection _sinkRecordCollection;
    private readonly PauseTokenSource _pauseTokenSource;

    public SinkTask(
        ISinkExceptionHandler sinkExceptionHandler,
        IConfigurationProvider configurationProvider,
        IExecutionContext executionContext,
        IConnectRecordCollection sinkRecordCollection)
    {
        _sinkExceptionHandler = sinkExceptionHandler;
        _configurationProvider = configurationProvider;
        _executionContext = executionContext;
        _sinkRecordCollection = sinkRecordCollection;
        _pauseTokenSource = PauseTokenSource.New();
    }

    public bool IsPaused => false;
    public bool IsStopped { get; private set; }

    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        _executionContext.Initialize(connector, taskId, this);
        
        using (LogContext.PushProperty("GroupId", _configurationProvider.GetGroupId(connector)))
        {
            var batchPollContext = _executionContext.GetOrSetBatchContext(connector, taskId, cts.Token);
            _sinkRecordCollection.Setup(connector, taskId);
            if (!_sinkRecordCollection.TrySubscribe())
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
                _sinkRecordCollection.Clear();
                using (LogContext.PushProperty("Batch", batchPollContext.Iteration))
                {
                    try
                    {
                        await _sinkRecordCollection.Consume();
                        await _sinkRecordCollection.Process();
                        await _sinkRecordCollection.Sink();
                        _sinkRecordCollection.Commit();
                    }
                    catch (Exception ex)
                    {
                        if (_configurationProvider.IsErrorTolerated(connector))
                        {
                            await _sinkRecordCollection.DeadLetter(ex);
                            _sinkRecordCollection.Commit();
                        }
                        _sinkExceptionHandler.Handle(ex, Cancel);
                    }
                    finally
                    {   
                        _sinkRecordCollection.Record();
                        await _sinkRecordCollection.NotifyEndOfPartition();
                    }
                }
            }

            _sinkRecordCollection.Cleanup();
        }

        IsStopped = true;
        return;

        void Cancel()
        {
            if (!_configurationProvider.IsErrorTolerated(connector))
            {
                cts.Cancel();
            }
        }
    }
}