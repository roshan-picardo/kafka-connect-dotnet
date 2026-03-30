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
    private Timer _timer;
    private readonly PauseTokenSource _pauseTokenSource = new();

    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
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
                    var leaderConfig = configurationProvider.GetLeaderConfig();
                    _timer.Interval = leaderConfig.MaxPollIntervalMs.GetValueOrDefault() - 100;

                    await workerRecordCollection.Consume(cts.Token);
                    
                    if (workerRecordCollection.Count() > 0)
                    {
                        logger.Info($"Received {workerRecordCollection.Count()} configuration change(s) from Kafka topic.");
                        
                        // Process and deserialize the configuration records
                        await workerRecordCollection.Process();

                        // Save configurations to local files (this writes to the settings directory)
                        await workerRecordCollection.Store();

                        logger.Info("Configuration changes saved to local files successfully.");
                        
                        // Trigger Worker to reload configurations and adjust connectors
                        await executionContext.Restart(2000);
                    }

                    workerRecordCollection.Commit();
                    _pauseTokenSource.Pause();

                    if (!_timer.Enabled)
                    {
                        _timer.Enabled = true;
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
