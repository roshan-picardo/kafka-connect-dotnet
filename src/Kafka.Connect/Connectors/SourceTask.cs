using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Connectors;

public class SourceTask(
    ILogger<SourceTask> logger,
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider,
    IConnectRecordCollection pollRecordCollection)
    : ISourceTask
{
    private readonly PauseTokenSource _pauseTokenSource = PauseTokenSource.New();

    public bool IsPaused => false;
    public bool IsStopped { get; private set; }

    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        executionContext.Initialize(connector, taskId, this);

        pollRecordCollection.Setup(ConnectorType.Source, connector, taskId);
        if (!(pollRecordCollection.TrySubscribe() && pollRecordCollection.TryPublisher()))
        {
            IsStopped = true;
            return;
        }

        while (!cts.IsCancellationRequested)
        {
            await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);

            if (cts.IsCancellationRequested) break;

            using (ConnectLog.Batch())
            {
                await pollRecordCollection.Consume(cts.Token);

                if (cts.IsCancellationRequested) break;

                var (timeOut, commands) = await pollRecordCollection.GetCommands();

                var timeOutWatch = Stopwatch.StartNew();
                await commands.ForEachAsync(configurationProvider.GetDegreeOfParallelism(connector), async cr =>
                {
                    if (cr is not CommandRecord record) return;
                    using (ConnectLog.Command(record.Name))
                    {
                        try
                        {
                            await pollRecordCollection.Source(record);
                            await pollRecordCollection.Process(record.Id.ToString());
                            await pollRecordCollection.Produce(record.Id.ToString());
                            await pollRecordCollection.UpdateCommand(record);
                        }
                        catch (Exception ex)
                        {
                            logger.Critical("FAILED", ex);
                        }
                        finally
                        {
                            pollRecordCollection.Record();
                            pollRecordCollection.Record(record.Id.ToString());
                        }

                        pollRecordCollection.Clear(record.Id.ToString());
                    }
                });
                pollRecordCollection.Commit(commands);
                pollRecordCollection.Clear();

                var pendingTime = timeOut - (int)timeOutWatch.ElapsedMilliseconds;
                timeOutWatch.Stop();
                if (pendingTime > 0)
                {
                    await Task.Delay(pendingTime);
                }
            }
        }

        IsStopped = true;
    }
}
