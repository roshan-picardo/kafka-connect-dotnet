using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Handlers;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Connectors;

public class SourceTask : ISourceTask
{
    private readonly ILogger<SourceTask> _logger;
    private readonly IExecutionContext _executionContext;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IConnectRecordCollection _pollRecordCollection;
    private readonly PauseTokenSource _pauseTokenSource;

    public SourceTask(
        ILogger<SourceTask> logger, 
        IExecutionContext executionContext,
        IConfigurationProvider configurationProvider,
        IConnectRecordCollection pollRecordCollection)
    {
        _logger = logger;
        _executionContext = executionContext;
        _configurationProvider = configurationProvider;
        _pollRecordCollection = pollRecordCollection;
        _pauseTokenSource = PauseTokenSource.New();
    }
    
    public bool IsPaused => false;
    public bool IsStopped { get; private set; }

    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        _executionContext.Initialize(connector, taskId, this);

        using (LogContext.Push(new PropertyEnricher("GroupId", _configurationProvider.GetGroupId(connector))))
        {
            var batchPollContext = _executionContext.GetOrSetBatchContext(connector, taskId, cts.Token);
            _pollRecordCollection.Setup(connector, taskId);
            if (!(_pollRecordCollection.TrySubscribe() && _pollRecordCollection.TryPublisher()))
            {
                IsStopped = true;
                return;
            }

            while (!cts.IsCancellationRequested)
            {
                await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);
                
                if (cts.IsCancellationRequested) break;

                batchPollContext.Reset(_executionContext.GetNextPollIndex());
                using (LogContext.PushProperty("Batch", batchPollContext.Iteration))
                {
                    _logger.Critical($"Start Iteration: {batchPollContext.Iteration}");
                    await _pollRecordCollection.Consume();
                    
                    if(cts.IsCancellationRequested) break;

                    var configCommands = await _pollRecordCollection.GetCommands(connector);

                    var timeOutWatch = Stopwatch.StartNew();
                    await configCommands.Commands.ForEachAsync(_configurationProvider.GetDegreeOfParallelism(connector), async cr =>
                        {
                            if (cr is not CommandRecord record) return;
                            try
                            {
                                await _pollRecordCollection.Source(record);
                                await _pollRecordCollection.Process(record.Id);
                                //await sourceRecordCollection.Produce();
                                await _pollRecordCollection.UpdateCommand(record);
                            }
                            catch (Exception ex)
                            {
                                _logger.Critical("FAILED", ex);
                            }
                            finally
                            {
                                _pollRecordCollection.Record(record.Id);
                            }
                            _pollRecordCollection.Clear(record.Id);
                        });
                    _pollRecordCollection.Commit(configCommands.Commands);
                    _pollRecordCollection.Clear();

                    _logger.Critical($"End Iteration: {batchPollContext.Iteration}");
                    var pendingTime = configCommands.TimeOut - (int)timeOutWatch.ElapsedMilliseconds;
                    timeOutWatch.Stop();
                    if (pendingTime > 0)
                    {
                        _logger.Critical($"Applying Delay: {batchPollContext.Iteration}");
                        await Task.Delay(pendingTime);
                    }
                }
            }
        }

        IsStopped = true;
    }
}
