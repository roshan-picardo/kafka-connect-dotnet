using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Connectors;

public class LeaderTask(
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider,
    IConnectRecordCollection leaderRecordCollection,
    ILogger<LeaderTask> logger)
    : ILeaderTask
{
    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        executionContext.Initialize(connector, taskId, this);
        await leaderRecordCollection.Setup(ConnectorType.Leader, connector, taskId);
        
        if (!(leaderRecordCollection.TrySubscribe() && leaderRecordCollection.TryPublisher()))
        {
            IsStopped = true;
            return;
        }

        var consumerTask = Consume(connector, taskId, cts);
        var publisherTask = Publish(connector, cts);

        await Task.WhenAll(consumerTask, publisherTask);

        leaderRecordCollection.Cleanup();
        IsStopped = true;
    }

    private async Task Consume(string connector, int taskId, CancellationTokenSource cts)
    {
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
    }

    private async Task Publish(string connector, CancellationTokenSource cts)
    {
        var reader = executionContext.ConfigurationChannel.Reader;
        
        while (!cts.IsCancellationRequested)
        {
            try
            {
                var configuration = await reader.ReadAsync(cts.Token);
                leaderRecordCollection.Configure(connector, configuration);
                await leaderRecordCollection.Process(connector);
                await leaderRecordCollection.Produce(connector);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.Error("Publisher loop exception", ex);
            }
            finally
            {
                leaderRecordCollection.Record(connector);
            }
        }
    }

    public bool IsPaused => false;
    public bool IsStopped { get; private set; }
}
