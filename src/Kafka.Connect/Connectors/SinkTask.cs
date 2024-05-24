using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Serilog.Context;

namespace Kafka.Connect.Connectors;

public class SinkTask(
    ISinkExceptionHandler sinkExceptionHandler,
    IConfigurationProvider configurationProvider,
    IExecutionContext executionContext,
    IConnectRecordCollection sinkRecordCollection)
    : ISinkTask
{
    private readonly PauseTokenSource _pauseTokenSource = PauseTokenSource.New();

    public bool IsPaused => false;
    public bool IsStopped { get; private set; }

    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        executionContext.Initialize(connector, taskId, this);

        sinkRecordCollection.Setup(ConnectorType.Sink, connector, taskId);
        if (!sinkRecordCollection.TrySubscribe())
        {
            IsStopped = true;
            return;
        }

        while (!cts.IsCancellationRequested)
        {
            await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);
            //TODO: lets approach this solution differently - need an Admin node to issue pause / resume over all workers.
            if (cts.IsCancellationRequested) break;

            sinkRecordCollection.Clear();
            using (ConnectLog.Batch())
            {
                try
                {
                    await sinkRecordCollection.Consume(cts.Token);
                    await sinkRecordCollection.Process();
                    await sinkRecordCollection.Sink();
                    sinkRecordCollection.Commit();
                }
                catch (Exception ex)
                {
                    if (configurationProvider.IsErrorTolerated(connector))
                    {
                        await sinkRecordCollection.DeadLetter(ex);
                        sinkRecordCollection.Commit();
                    }

                    sinkExceptionHandler.Handle(ex, Cancel);
                }
                finally
                {
                    sinkRecordCollection.Record();
                    await sinkRecordCollection.NotifyEndOfPartition();
                }
            }

            sinkRecordCollection.Cleanup();
        }

        IsStopped = true;
        return;

        void Cancel()
        {
            if (!configurationProvider.IsErrorTolerated(connector))
            {
                cts.Cancel();
            }
        }
    }
}