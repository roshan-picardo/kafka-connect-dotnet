using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;

namespace Kafka.Connect.Connectors;

public class SourceTask(
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider,
    IConnectRecordCollection pollRecordCollection, 
    ITokenHandler tokenHandler,
    ILogger<SourceTask> logger)
    : ISourceTask
{
    private readonly PauseTokenSource _pauseTokenSource = new();

    public bool IsPaused => false;
    public bool IsStopped { get; private set; }

    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        executionContext.Initialize(connector, taskId, this);
        
        await pollRecordCollection.Setup(ConnectorType.Source, connector, taskId);
        if (!(pollRecordCollection.TrySubscribe() && pollRecordCollection.TryPublisher()))
        {
            IsStopped = true;
            return;
        }

        var timeoutInMs = configurationProvider.GetBatchConfig(connector).TimeoutInMs;
        var parallelOptions = configurationProvider.GetParallelRetryOptions(connector);

        var attempts = parallelOptions.Attempts;

        while (!cts.IsCancellationRequested)
        {
            tokenHandler.NoOp();
            await _pauseTokenSource.WaitUntilTimeout(Interlocked.Exchange(ref timeoutInMs, configurationProvider.GetBatchConfig(connector).TimeoutInMs), cts.Token);

            if (cts.IsCancellationRequested) break;

            using (ConnectLog.Batch())
            {
                try
                {
                    await pollRecordCollection.Consume(cts.Token);

                    if (cts.IsCancellationRequested) break;

                    var commands = await pollRecordCollection.GetCommands();
                    executionContext.UpdateCommands(connector, taskId, commands);

                    await commands.ForEachAsync(parallelOptions, async cr =>
                    {
                        if (cr is not CommandRecord record) return;
                        using (ConnectLog.Command(record.Name))
                        {
                            try
                            {
                                record.Status = Status.Sourcing;
                                await pollRecordCollection.Source(record);
                                await pollRecordCollection.Process(record.Id.ToString());
                                await pollRecordCollection.Produce(record.Id.ToString());
                                record.Status = Status.Sourced;
                            }
                            catch (Exception ex)
                            {
                                record.Status = Status.Failed;
                                record.Exception = ex is not ConnectAggregateException ? ex : null;
                            }
                            finally
                            {
                                if (configurationProvider.IsDeadLetterEnabled(connector))
                                {
                                    await pollRecordCollection.DeadLetter(record.Id.ToString());
                                }

                                pollRecordCollection.Record(record);
                                await pollRecordCollection.UpdateCommand(record);

                                if (pollRecordCollection.Count(record.Id.ToString()) >= record.BatchSize)
                                {
                                    Interlocked.Exchange(ref timeoutInMs, 0);
                                }
                            }

                            pollRecordCollection.Clear(record.Id.ToString());
                        }
                    });
                    if (!cts.IsCancellationRequested)
                    {
                        pollRecordCollection.Commit(commands);
                    }

                    pollRecordCollection.Clear();
                    attempts = parallelOptions.Attempts;
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
            }
        }
        IsStopped = true;
    }
}
