using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Connectors;

public class LeaderSettingsConsumerSubTask(
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider,
    IConnectRecordCollection leaderRecordCollection,
    ILogger<LeaderSettingsConsumerSubTask> logger)
    : ILeaderSubTask
{
    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        logger.Info($"Staring leader task: {connector},  taskId: {taskId:00}");
        executionContext.Initialize(connector, taskId, this);
        await leaderRecordCollection.Setup(ConnectorType.Leader, connector, taskId);
        
        if (!leaderRecordCollection.TrySubscribe())
        {
            IsStopped = true;
            return;
        }

        var parallelOptions = configurationProvider.GetParallelRetryOptions(connector);
        var attempts = parallelOptions.Attempts;
        var refresh = true;

        while (!cts.IsCancellationRequested)
        {
            leaderRecordCollection.ClearAll();
            if (cts.IsCancellationRequested) break;

            leaderRecordCollection.Clear();
            using (ConnectLog.Batch())
            {
                try
                {
                    await leaderRecordCollection.Consume(cts.Token);
                    await leaderRecordCollection.Process();
                    await leaderRecordCollection.Store(refresh);
                    if (refresh)
                    {
                        refresh = false;
                    }

                    attempts = parallelOptions.Attempts;
                }
                catch (Exception ex)
                {
                    --attempts;
                    logger.Critical($"Consumer loop exception. Attempts remaining: {attempts}", ex);
                    if (attempts == 0)
                    {
                        await cts.CancelAsync();
                    }
                }
                finally
                {
                    leaderRecordCollection.Record();
                }
            }
        }

        leaderRecordCollection.Cleanup();
        IsStopped = true;
    }

    public bool IsPaused => false;
    public bool IsStopped { get; private set; }
}
