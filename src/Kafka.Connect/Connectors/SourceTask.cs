using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;

namespace Kafka.Connect.Connectors;

public class SourceTask(
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider,
    IConnectRecordCollection pollRecordCollection,
    ISinkExceptionHandler sinkExceptionHandler)
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

        while (!cts.IsCancellationRequested)
        {
            await _pauseTokenSource.WaitUntilTimeout(Interlocked.Exchange(ref timeoutInMs, configurationProvider.GetBatchConfig(connector).TimeoutInMs), cts.Token);

            if (cts.IsCancellationRequested) break;

            using (ConnectLog.Batch())
            {
                await pollRecordCollection.Consume(cts.Token);

                if (cts.IsCancellationRequested) break;

                var commands = await pollRecordCollection.GetCommands();
                executionContext.UpdateCommands(connector, taskId, commands);

                await commands.ForEachAsync(configurationProvider.GetDegreeOfParallelism(connector), async cr =>
                {
                    if (cr is not CommandRecord record) return;
                    using (ConnectLog.Command(record.Name))
                    {
                        try
                        {
                            pollRecordCollection.UpdateTo(SinkStatus.Sourcing, record.Topic, record.Partition, record.Offset);
                            await pollRecordCollection.Source(record);
                            await pollRecordCollection.Process(record.Id.ToString());
                            await pollRecordCollection.Produce(record.Id.ToString());
                            pollRecordCollection.UpdateTo(SinkStatus.Sourced, record.Topic, record.Partition, record.Offset);
                        }
                        catch (Exception ex)
                        {
                            pollRecordCollection.UpdateTo(SinkStatus.Failed, record.Topic, record.Partition, record.Offset, 
                                ex is not ConnectAggregateException ? ex : null);

                            if (configurationProvider.IsErrorTolerated(connector))
                            {
                                await pollRecordCollection.DeadLetter(ex, record.Id.ToString());
                            }

                            sinkExceptionHandler.Handle(ex, Cancel);
                        }
                        finally
                        {
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
            }
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
