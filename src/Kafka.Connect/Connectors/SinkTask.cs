using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;

namespace Kafka.Connect.Connectors;

public class SinkTask(
    IConfigurationProvider configurationProvider,
    IExecutionContext executionContext,
    IConnectRecordCollection sinkRecordCollection, 
    ITokenHandler tokenHandler,
    ILogger<SinkTask> logger)
    : ISinkTask
{
    private readonly PauseTokenSource _pauseTokenSource = PauseTokenSource.New();

    public bool IsPaused => false;
    public bool IsStopped { get; private set; }

    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        executionContext.Initialize(connector, taskId, this);

        await sinkRecordCollection.Setup(ConnectorType.Sink, connector, taskId);
        if (!sinkRecordCollection.TrySubscribe())
        {
            IsStopped = true;
            return;
        }

        var parallelOptions = configurationProvider.GetParallelRetryOptions(connector);
        var attempts = parallelOptions.Attempts;

        while (!cts.IsCancellationRequested)
        {
            tokenHandler.NoOp();
            await _pauseTokenSource.WaitWhilePaused(cts.Token);
            if (cts.IsCancellationRequested) break;

            sinkRecordCollection.Clear();
            using (ConnectLog.Batch())
            {
                try
                {
                    await sinkRecordCollection.Consume(cts.Token);
                    await sinkRecordCollection.Process();
                    await sinkRecordCollection.Sink();
                }
                catch (Exception ex)
                {
                    attempts--;
                    logger.Critical($"Unhandled exception has occured. Attempts remaining: {attempts}", ex);
                    if (attempts == 0)
                    {
                        await cts.CancelAsync();
                    }
                }
                finally
                {
                    if (configurationProvider.IsDeadLetterEnabled(connector))
                    {
                        await sinkRecordCollection.DeadLetter();
                    }
                    sinkRecordCollection.Commit();
                    sinkRecordCollection.Record();
                    await sinkRecordCollection.NotifyEndOfPartition();
                }

                attempts = parallelOptions.Attempts;
            }
            sinkRecordCollection.Clear();
        }
        sinkRecordCollection.Cleanup();
        IsStopped = true;
    }
}


