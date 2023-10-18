using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Handlers;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
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
    private readonly ISourceProcessor _sourceProcessor;
    private readonly ITokenHandler _tokenHandler;
    private IProducer<byte[], byte[]> _producer;
    private IConsumer<byte[], byte[]> _consumer;
    private readonly PauseTokenSource _pauseTokenSource;

    public SourceTask(
        ILogger<SourceTask> logger, 
        IExecutionContext executionContext,
        IConfigurationProvider configurationProvider,
        ISourceProducer sourceProducer,
        ISourceHandler sourceHandler,
        ISinkConsumer sinkConsumer,
        ISourceProcessor sourceProcessor,
        ITokenHandler tokenHandler)
    {
        _logger = logger;
        _executionContext = executionContext;
        _configurationProvider = configurationProvider;
        _sourceProducer = sourceProducer;
        _sourceHandler = sourceHandler;
        _sinkConsumer = sinkConsumer;
        _sourceProcessor = sourceProcessor;
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

        // TODO: move it a bit down
        _producer = _sourceProducer.GetProducer(connector, taskId);
        if (_producer == null)
        {
            _logger.Warning("Failed to create the consumer, exiting from the sink task.");
            IsStopped = true;
            return;
        }
        
        // register the consumer
        _consumer = _sinkConsumer.Subscribe(connector, taskId);
        if (_consumer == null)
        {
            _logger.Warning("Failed to create the consumer, exiting from the sink task.");
            IsStopped = true;
            return;
        }

        using (LogContext.Push(new PropertyEnricher("GroupId", _configurationProvider.GetGroupId(connector))))
        {
            var batchPollContext = _executionContext.GetOrSetBatchContext(connector, taskId, cts.Token);

            while (!cts.IsCancellationRequested)
            {
                _tokenHandler.DoNothing();
                await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);

                if (cts.IsCancellationRequested) break;

                batchPollContext.Reset(_executionContext.GetNextPollIndex());
                using (LogContext.PushProperty("Batch", batchPollContext.Iteration))
                {
                    try
                    {
                        var triggerBatch = await _sinkConsumer.Consume(_consumer, connector, taskId, true);
                        if (triggerBatch.IsEmpty)
                        {
                            //_logger.Error($"All Partitions are empty! {triggerBatch.EofCount}");
                            // Write only the required message based on the target config
                        }

                        var commandContexts = await _sourceProcessor.Process(triggerBatch, connector);
                        var batches = new Dictionary<string, ConnectRecordBatch>();
                        await commandContexts.ForEachAsync( command =>
                        {
                            _logger.Warning($"Loading for {command.Topic}");
                            var batch = new ConnectRecordBatch(connector);
                            batches.Add(command.Topic, batch);
                            //query database and add result to batch
                            // process the records
                            // serialize the records
                            // produce messages to command.topic
                            
                            
                            // note the last successful timestamp
                            // create new command context and publish tracking message - this should be produced to the exact partition
                            // commit the command.offset
                            return Task.CompletedTask;
                        }, (context, exception) => exception.SetLogContext(batches[context.Topic]));
                    }
                    catch (Exception ex)
                    {
                        //_sinkExceptionHandler.Handle(ex, Cancel);
                    }
                    finally
                    {
                        // if (!cts.IsCancellationRequested)
                        // {
                        //     await CommitAndLog(batch, connector, taskId);
                        // }
                        
                    }
                }
            }

            //if(cts.IsCancellationRequested) continue;
            // do the retries!!
        }

        IsStopped = true;
    }
}