using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Timer = System.Timers.Timer;

namespace Kafka.Connect.Connectors;

public class LeaderTask(
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider,
    IConnectRecordCollection leaderRecordCollection,
    ILogger<LeaderTask> logger)
    : ILeaderTask
{
    private FileSystemWatcher _fileSystemWatcher;
    private Timer _timer;
    private readonly PauseTokenSource _pauseTokenSource = new();

    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        executionContext.Initialize(connector, taskId, this);
        await leaderRecordCollection.Setup(ConnectorType.Leader, connector, taskId);
        if (!(leaderRecordCollection.TrySubscribe() && leaderRecordCollection.TryPublisher()))
        {
            IsStopped = true;
            return;
        }

        Trigger();
        var parallelOptions = configurationProvider.GetParallelRetryOptions(connector);
        var attempts = parallelOptions.Attempts;

        while (!cts.IsCancellationRequested)
        {
            leaderRecordCollection.ClearAll();
            await _pauseTokenSource.WaitWhilePaused(cts.Token);
            if (cts.IsCancellationRequested) break;

            leaderRecordCollection.Clear();
            using (ConnectLog.Batch())
            {
                try
                {
                    _timer.Interval = configurationProvider.GetLeaderConfig().MaxPollIntervalMs.GetValueOrDefault() - 100;

                    await leaderRecordCollection.Consume(cts.Token);
                    await leaderRecordCollection.Process();

                    await leaderRecordCollection.Configure(connector, _fileSystemWatcher.EnableRaisingEvents);

                    await leaderRecordCollection.Process(connector);
                    await leaderRecordCollection.Produce(connector);
                    leaderRecordCollection.UpdateTo(Status.Reviewed, connector);

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

                    attempts = parallelOptions.Attempts;
                }
                catch (Exception ex)
                {
                    --attempts;
                    logger.Critical($"Unhandled exception has occured. Attempts remaining: {attempts}", ex);
                    if (attempts == 0)
                    {
                        await cts.CancelAsync();
                    }
                }
                finally
                {
                    leaderRecordCollection.Record();
                    leaderRecordCollection.Record(connector);
                }
            }
        }

        leaderRecordCollection.Cleanup();

        IsStopped = true;
    }

    public bool IsPaused => false;
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
