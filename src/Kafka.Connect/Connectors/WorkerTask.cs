using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Timer = System.Timers.Timer;

namespace Kafka.Connect.Connectors;

public class WorkerTask(
    IConfigurationProvider configurationProvider,
    IExecutionContext executionContext,
    IConnectRecordCollection workerRecordCollection,
    ILogger<WorkerTask> logger)
    : IWorkerTask
{
    private readonly PauseTokenSource _pauseTokenSource = new();

    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        logger.Info($"Staring worker task: {connector},  taskId: {taskId:00}");
        executionContext.Initialize(connector, taskId, this);
        await workerRecordCollection.Setup(ConnectorType.Worker, connector, taskId);
        
        if (!workerRecordCollection.TrySubscribe())
        {
            IsStopped = true;
            return;
        }

        var parallelOptions = configurationProvider.GetParallelRetryOptions(connector);
        var attempts = parallelOptions.Attempts;

        while (!cts.IsCancellationRequested)
        {
            workerRecordCollection.ClearAll();
            await _pauseTokenSource.WaitWhilePaused(cts.Token);
            if (cts.IsCancellationRequested) break;

            workerRecordCollection.Clear();
            using (ConnectLog.Batch())
            {
                try
                {
                    await workerRecordCollection.Consume(cts.Token);
                    
                    if (workerRecordCollection.Count() > 0)
                    {
                        await workerRecordCollection.Process();
                        
                        await workerRecordCollection.Store(configurationProvider.GetNodeName());
                        
                        await workerRecordCollection.Refresh(configurationProvider.GetNodeName());
                    }

                    attempts = parallelOptions.Attempts;
                }
                catch (Exception ex)
                {
                    --attempts;
                    logger.Critical($"Unhandled exception occurred in WorkerTask. Attempts remaining: {attempts}", ex);
                    if (attempts == 0)
                    {
                        await cts.CancelAsync();
                    }
                }
                finally
                {
                    workerRecordCollection.Record();
                    workerRecordCollection.Record(connector);
                }
            }
        }

        workerRecordCollection.Cleanup();
        IsStopped = true;
    }

    public bool IsPaused => _pauseTokenSource.IsPaused;
    public bool IsStopped { get; private set; }
}
