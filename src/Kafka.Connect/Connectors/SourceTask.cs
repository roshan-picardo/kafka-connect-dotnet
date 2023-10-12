using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Kafka.Connect.Utilities;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Connectors;

public class SourceTask : ISourceTask
{
    private readonly ILogger<SourceTask> _logger;
    private readonly IExecutionContext _executionContext;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly ISourceProducer _sourceProducer;
    private readonly ISourceHandler _sourceHandler;
    private readonly ISinkConsumer _sinkConsumer;
    private readonly ITokenHandler _tokenHandler;
    private IProducer<byte[], byte[]> _producer;
    private readonly PauseTokenSource _pauseTokenSource;

    public SourceTask(
        ILogger<SourceTask> logger, 
        IExecutionContext executionContext,
        IConfigurationProvider configurationProvider,
        ISourceProducer sourceProducer,
        ISourceHandler sourceHandler,
        ISinkConsumer sinkConsumer,
        ITokenHandler tokenHandler)
    {
        _logger = logger;
        _executionContext = executionContext;
        _configurationProvider = configurationProvider;
        _sourceProducer = sourceProducer;
        _sourceHandler = sourceHandler;
        _sinkConsumer = sinkConsumer;
        _tokenHandler = tokenHandler;
        _pauseTokenSource = PauseTokenSource.New();
    }
    
    public bool IsPaused => false;
    public bool IsStopped { get; private set; }
    
    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        void Cancel()
        {
            if (!_configurationProvider.IsErrorTolerated(connector))
            {
                cts.Cancel();
            }
        }

        _executionContext.Initialize(connector, taskId, this);

        _producer = _sourceProducer.GetProducer(connector, taskId);
        if (_producer == null)
        {
            _logger.Warning("Failed to create the consumer, exiting from the sink task.");
            IsStopped = true;
            return;
        }

        using (LogContext.Push(new PropertyEnricher("GroupId", _configurationProvider.GetGroupId(connector)),
                   new PropertyEnricher("Producer", _producer.Name?.Replace(connector, ""))))
        {
            var batchPollContext = _executionContext.GetOrSetBatchContext(connector, taskId, cts.Token);

            while (!cts.IsCancellationRequested)
            {
                _tokenHandler.DoNothing();

                // subscribe to source map topic
                var consumer = _sinkConsumer.Subscribe(connector, taskId);
                while (!cts.IsCancellationRequested)
                {
                    var consumed = consumer.Consume(cts.Token);
                }
                

                await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);
                if (cts.IsCancellationRequested) break;

                batchPollContext.Reset(_executionContext.GetNextPollIndex());
                SinkRecordBatch batch = null; 
                using (LogContext.PushProperty("Batch", batchPollContext.Iteration))
                {
                    try
                    {
                        
                        // star the timer 
                        //batch = _sourceHandler.Get(); // this should load the data in here;
                        // Read the records from the database - this should be source handlers job
                        // Apply the message processors - the same way we have them for sink
                        // serialise the message
                        // start producing them one after another
                        // wait for the timer to end
                        // repeat
                    }
                    catch (Exception ex)
                    {
                        //_sinkExceptionHandler.Handle(ex, Cancel);
                    }
                    finally
                    {
                        _logger.Record(batch, _configurationProvider.GetLogEnhancer(connector), connector);
                        // if (!cts.IsCancellationRequested)
                        // {
                        //     await CommitAndLog(batch, connector, taskId);
                        // }
                        _executionContext.AddToCount(batch?.Count ?? 0);
                        
                        _logger.Debug("Finished processing the batch.",
                            new
                            {
                                Records = batch?.Count ?? 0,
                                Duration = _executionContext.GetOrSetBatchContext(connector, taskId).Timer.EndTiming(),
                                Stats = batch?.GetBatchStatus()
                            });
                    }
                }
            }

            //if(cts.IsCancellationRequested) continue;
            // do the retries!!
        }

        IsStopped = true;
    }
}