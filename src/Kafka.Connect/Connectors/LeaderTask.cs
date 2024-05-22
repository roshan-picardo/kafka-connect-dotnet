using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Tokens;
using Serilog.Context;
using IConfigurationProvider = Kafka.Connect.Providers.IConfigurationProvider;
using Timer = System.Timers.Timer;

namespace Kafka.Connect.Connectors;

public class LeaderTask(
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider,
    IConnectRecordCollection leaderRecordCollection,
    ISinkExceptionHandler sinkExceptionHandler)
    : ILeaderTask
{
    private FileSystemWatcher _fileSystemWatcher;
    private Timer _timer;
    private readonly PauseTokenSource _pauseTokenSource = new();

    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        executionContext.Initialize(connector, taskId, this);
        using (LogContext.PushProperty("GroupId", configurationProvider.GetGroupId(connector)))
        {
            var batchPollContext = executionContext.GetOrSetBatchContext(connector, taskId, cts.Token);
            leaderRecordCollection.Setup(ConnectorType.Leader, connector, taskId);
            if (!(leaderRecordCollection.TrySubscribe() && leaderRecordCollection.TryPublisher()))
            {
                IsStopped = true;
                return;
            }
            
            Trigger();

            while (!cts.IsCancellationRequested)
            {
                leaderRecordCollection.ClearAll();
                await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);
                if (cts.IsCancellationRequested) break;

                batchPollContext.Reset(executionContext.GetNextPollIndex());
                leaderRecordCollection.Clear();
                using (LogContext.PushProperty("Batch", batchPollContext.Iteration))
                {
                    try
                    {
                        _timer.Interval = configurationProvider.GetLeaderConfig().MaxPollIntervalMs.GetValueOrDefault() - 100;

                        await leaderRecordCollection.Consume();
                        await leaderRecordCollection.Process();
                        
                        await leaderRecordCollection.Configure(connector, _fileSystemWatcher.EnableRaisingEvents);
                        
                        await leaderRecordCollection.Process(connector);
                        await leaderRecordCollection.Produce(connector);
                        leaderRecordCollection.UpdateTo(SinkStatus.Reviewed, connector);
                        
                        leaderRecordCollection.Commit();
                        _pauseTokenSource.Pause();
                        if (!_fileSystemWatcher.EnableRaisingEvents)
                        {
                            _fileSystemWatcher.EnableRaisingEvents = true;
                        }

                        if (!_timer.Enabled)
                        {
                            _timer.Enabled = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        sinkExceptionHandler.Handle(ex, () => _pauseTokenSource.Pause());
                    }
                    finally
                    {   
                        leaderRecordCollection.Record();
                        leaderRecordCollection.Record(connector);
                    }
                }
            }

            leaderRecordCollection.Cleanup();
        }

        IsStopped = true;
    }

    public bool IsPaused { get; set; }
    public bool IsStopped { get; private set; }

    private void Trigger()
    {
        var leaderConfig = configurationProvider.GetLeaderConfig();
        _fileSystemWatcher = new FileSystemWatcher(leaderConfig.Settings, "*.json");
        _fileSystemWatcher.Changed += (_, _) => _pauseTokenSource.Resume();
        _fileSystemWatcher.Created += (_, _) => _pauseTokenSource.Resume();
        _fileSystemWatcher.Deleted += (_, _) => _pauseTokenSource.Resume();
        _fileSystemWatcher.Renamed += (_, _) => _pauseTokenSource.Resume();

        _timer = new Timer { Enabled = false };
        _timer.Elapsed += (_, _) => _pauseTokenSource.Resume();
    }
}