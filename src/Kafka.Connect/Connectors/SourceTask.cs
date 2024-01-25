using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
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
    private readonly IRetriableHandler _retriableHandler;
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
        IRetriableHandler retriableHandler,
        ITokenHandler tokenHandler)
    {
        _logger = logger;
        _executionContext = executionContext;
        _configurationProvider = configurationProvider;
        _sourceProducer = sourceProducer;
        _sourceHandler = sourceHandler;
        _sinkConsumer = sinkConsumer;
        _sourceProcessor = sourceProcessor;
        _retriableHandler = retriableHandler;
        _tokenHandler = tokenHandler;
        _pauseTokenSource = PauseTokenSource.New();
    }
    
    public bool IsPaused => false;
    public bool IsStopped { get; private set; }
    
    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        // void Cancel()
        // {
        //     if (!_configurationProvider.IsErrorTolerated(connector))
        //     {
        //         cts.Cancel();
        //     }
        // }

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
                        
                        //TODO: Not a working component as yet
                        var triggerBatch = await _sinkConsumer.Consume(_consumer, connector, taskId, true);

                        var commandContexts = await _sourceProcessor.Commands(null, connector);
                        var batches = new Dictionary<string, ConnectRecordBatch>();
                        await commandContexts.ForEachAsync(async command =>
                        {
                            // Run this in retriable..
                            // Should have read strategies - those understand the config and query the database
                            // Read strategies should also be able to create next at the end of processing.
                            
                            // Challenges - What if the boundary records have same condition - must have a mechanism to continue reading until next value? 
                            // what if same same condition value at boundary one fails but other succeeds..
                            var batch = await _sourceHandler.Get(connector, taskId);
                            batches.Add(command.Topic, batch);
                            // Process and Produce must go within retriable 

                            await _retriableHandler.Retry((b, c) => ProcessAndProduce(b, c, 10));
                            
                            var bbb = await _retriableHandler.Retry(b => ProcessAndProduce(b, connector, taskId), batch, connector);
                            
                            await _sourceProcessor.Process(batch, connector);
                            await _sourceProducer.Produce(_producer, connector, taskId, batch);
                            
                            // Get the last produced record and calculate next; must be done within 
                            await _sourceProducer.Produce(_producer, command);
                            _sinkConsumer.Commit(_consumer, command);
                        }, (context, exception) => exception.SetLogContext(batches[context.Topic]));
                    }
                    catch (Exception ex)
                    {
                        _logger.Critical("FAILED", ex);
                    }
                }
            }
        }
        IsStopped = true;
    }

    private async Task<ConnectRecordBatch> ProcessAndProduce(ConnectRecordBatch batch, string connector, int taskId)
    {
        await _sourceProcessor.Process(batch, connector);
        await _sourceProducer.Produce(_producer, connector, taskId, batch);
        return batch;
    }
}
