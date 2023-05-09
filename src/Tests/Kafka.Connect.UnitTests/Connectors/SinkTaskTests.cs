using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Connectors;
using Kafka.Connect.Handlers;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;
using SinkRecord = Kafka.Connect.Models.SinkRecord;

namespace UnitTests.Kafka.Connect.Connectors;

public class SinkTaskTests
{
    private readonly ILogger<SinkTask> _logger;
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly ISinkConsumer _sinkConsumer;
    private readonly ISinkProcessor _sinkProcessor;
    private readonly IPartitionHandler _partitionHandler;
    private readonly ISinkExceptionHandler _sinkExceptionHandler;
    private readonly IRetriableHandler _retriableHandler;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IExecutionContext _executionContext;
    private readonly ITokenHandler _tokenHandler;
    private readonly SinkTask _sinkTask;

    public SinkTaskTests()
    {
        _logger = Substitute.For<ILogger<SinkTask>>();
        _sinkConsumer = Substitute.For<ISinkConsumer>();
        _sinkProcessor = Substitute.For<ISinkProcessor>();
        _partitionHandler = Substitute.For<IPartitionHandler>();
        _sinkExceptionHandler = Substitute.For<ISinkExceptionHandler>();
        _retriableHandler = Substitute.For<IRetriableHandler>();
        _configurationProvider = Substitute.For<IConfigurationProvider>();
        _executionContext = Substitute.For<IExecutionContext>();
        _consumer = Substitute.For<IConsumer<byte[], byte[]>>();
        _tokenHandler = Substitute.For<ITokenHandler>();

        _sinkTask = new SinkTask(_logger, _sinkConsumer, _sinkProcessor, _partitionHandler, _sinkExceptionHandler,
            _retriableHandler, _configurationProvider, _executionContext, _tokenHandler);
    }

    [Fact]
    public async Task Execute_FailsToSubscribe()
    {
        _sinkConsumer.Subscribe(Arg.Any<string>(), Arg.Any<int>()).Returns((IConsumer<byte[], byte[]>)null);

        await _sinkTask.Execute("connector", 1, GetCancellationToken(1));
        
        _executionContext.Received().Initialize("connector", 1, _sinkTask);
        _sinkConsumer.Received().Subscribe("connector", 1);
        _logger.Received().Warning("Failed to create the consumer, exiting from the sink task.");
        Assert.True(_sinkTask.IsStopped);
    }


    [Fact]
    public async Task Execute_SimpleSuccess()
    {
        const string connector = "connector";
        const int taskId = 1;
        var cts = GetCancellationToken();
        var batch = new SinkRecordBatch("") { new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>()}) };
        _sinkConsumer.Subscribe(Arg.Any<string>(), Arg.Any<int>()).Returns(_consumer);
        _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new BatchPollContext());
        _executionContext.GetNextPollIndex().Returns(10);
        _sinkConsumer.Consume(Arg.Any<IConsumer<byte[], byte[]>>(), Arg.Any<string>(), Arg.Any<int>()).Returns(batch);
        _retriableHandler.Retry(Arg.Any<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(), Arg.Any<SinkRecordBatch>(), Arg.Any<string>()).Returns(batch);
        _configurationProvider.GetLogEnhancer(Arg.Any<string>()).Returns("Log.Provider");

        await _sinkTask.Execute(connector, taskId, cts);
        
        _executionContext.Received().Initialize(connector, taskId, _sinkTask);
        _sinkConsumer.Received().Subscribe(connector, taskId);
        _logger.DidNotReceive().Warning("Failed to create the consumer, exiting from the sink task.");
        _executionContext.Received().GetOrSetBatchContext(connector, taskId, cts.Token);
        _executionContext.Received().GetNextPollIndex();
        await _sinkConsumer.Received().Consume(_consumer, connector, taskId);
        await _retriableHandler.Received().Retry(Arg.Any<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(), batch, connector);
        _sinkExceptionHandler.DidNotReceive().Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        _partitionHandler.Received().CommitOffsets(batch, _consumer);
        _configurationProvider.Received().GetLogEnhancer(connector);
        _logger.Received().Record(batch, "Log.Provider", connector);
        await _partitionHandler.Received().NotifyEndOfPartition(batch, connector, taskId);
        _executionContext.Received().AddToCount(1);
        Assert.True(_sinkTask.IsStopped);
    }
    
    [Fact]
    public async Task Execute_ConsumeThrowsException()
    {
        const string connector = "connector";
        const int taskId = 1;
        var cts = GetCancellationToken();
        var batch = new SinkRecordBatch("") { new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>()}) };
        _sinkConsumer.Subscribe(Arg.Any<string>(), Arg.Any<int>()).Returns(_consumer);
        _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new BatchPollContext());
        _executionContext.GetNextPollIndex().Returns(10);
        _sinkConsumer.Consume(Arg.Any<IConsumer<byte[], byte[]>>(), Arg.Any<string>(), Arg.Any<int>()).Throws<Exception>();
        _retriableHandler.Retry(Arg.Any<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(), Arg.Any<SinkRecordBatch>(), Arg.Any<string>()).Returns(batch);
        _configurationProvider.GetLogEnhancer(Arg.Any<string>()).Returns("Log.Provider");

        await _sinkTask.Execute(connector, taskId, cts);
        
        _executionContext.Received().Initialize(connector, taskId, _sinkTask);
        _sinkConsumer.Received().Subscribe(connector, taskId);
        _logger.DidNotReceive().Warning("Failed to create the consumer, exiting from the sink task.");
        _executionContext.Received().GetOrSetBatchContext(connector, taskId, cts.Token);
        _executionContext.Received().GetNextPollIndex();
        await _sinkConsumer.Received().Consume(_consumer, connector, taskId);
        await _retriableHandler.DidNotReceive().Retry(Arg.Any<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(), batch, connector);
        _sinkExceptionHandler.Received().Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        _partitionHandler.Received().CommitOffsets(Arg.Any<SinkRecordBatch>(), _consumer);
        _configurationProvider.Received().GetLogEnhancer(connector);
        _logger.Received().Record(Arg.Any<SinkRecordBatch>(), "Log.Provider", connector);
        await _partitionHandler.Received().NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), connector, taskId);
        _executionContext.Received().AddToCount(0);
        Assert.True(_sinkTask.IsStopped);
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(true, false)]
    public async void Execute_ConsumeThrowsExceptionCancelInvoked(bool errorTolerated, bool tokenCancelled)
    {
        const string connector = "connector";
        const int taskId = 1;
        var cts = GetCancellationToken();
        var batch = new SinkRecordBatch("") { new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>()}) };
        _sinkConsumer.Subscribe(Arg.Any<string>(), Arg.Any<int>()).Returns(_consumer);
        _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new BatchPollContext());
        _executionContext.GetNextPollIndex().Returns(10);
        _sinkConsumer.Consume(Arg.Any<IConsumer<byte[], byte[]>>(), Arg.Any<string>(), Arg.Any<int>()).Throws<Exception>();
        _retriableHandler.Retry(Arg.Any<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(), Arg.Any<SinkRecordBatch>(), Arg.Any<string>()).Returns(batch);
        _configurationProvider.GetLogEnhancer(Arg.Any<string>()).Returns("Log.Provider");
        _configurationProvider.IsErrorTolerated(connector).Returns(errorTolerated);
        _sinkExceptionHandler.Handle(Arg.Any<Exception>(), Arg.Do<Action>(x => x.Invoke()));

        await _sinkTask.Execute(connector, taskId, cts);
        
        _executionContext.Received().Initialize(connector, taskId, _sinkTask);
        _sinkConsumer.Received().Subscribe(connector, taskId);
        _logger.DidNotReceive().Warning("Failed to create the consumer, exiting from the sink task.");
        _executionContext.Received().GetOrSetBatchContext(connector, taskId, cts.Token);
        _executionContext.Received().GetNextPollIndex();
        await _sinkConsumer.Received().Consume(_consumer, connector, taskId);
        await _retriableHandler.DidNotReceive().Retry(Arg.Any<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(), batch, connector);
        _sinkExceptionHandler.Received().Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        _partitionHandler.Received().CommitOffsets(Arg.Any<SinkRecordBatch>(), _consumer);
        _configurationProvider.Received().GetLogEnhancer(connector);
        _logger.Received().Record(Arg.Any<SinkRecordBatch>(), "Log.Provider", connector);
        await _partitionHandler.Received().NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), connector, taskId);
        _executionContext.Received().AddToCount(0);
        Assert.True(_sinkTask.IsStopped);
        
        // this assert assumes that token cancelled outside token handler
        _tokenHandler.Received(tokenCancelled ? 1 : 2).DoNothing();

    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async void Execute_ProcessAndSinkInternalWhenBatchIsNullOrEmpty(bool isNull)
    {
        const string connector = "connector";
        const int taskId = 1;
        var cts = GetCancellationToken();
        var batch = isNull ?  null : new SinkRecordBatch("");
        _sinkConsumer.Subscribe(Arg.Any<string>(), Arg.Any<int>()).Returns(_consumer);
        _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new BatchPollContext());
        _executionContext.GetNextPollIndex().Returns(10);
        _sinkConsumer.Consume(Arg.Any<IConsumer<byte[], byte[]>>(), Arg.Any<string>(), Arg.Any<int>()).Returns((SinkRecordBatch)null);
        _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async d=> batch = await d.Invoke(null)), Arg.Any<SinkRecordBatch>(), Arg.Any<string>()).Returns((SinkRecordBatch)null);
        _configurationProvider.GetLogEnhancer(Arg.Any<string>()).Returns("Log.Provider");

        await _sinkTask.Execute(connector, taskId, cts);
        
        _executionContext.Received().Initialize(connector, taskId, _sinkTask);
        _sinkConsumer.Received().Subscribe(connector, taskId);
        _logger.DidNotReceive().Warning("Failed to create the consumer, exiting from the sink task.");
        _executionContext.Received().GetOrSetBatchContext(connector, taskId, cts.Token);
        _executionContext.Received().GetNextPollIndex();
        await _sinkConsumer.Received().Consume(_consumer, connector, taskId);
        await _retriableHandler.Received().Retry(Arg.Any<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(), batch, connector);
        _partitionHandler.Received().CommitOffsets(batch, _consumer);
        _configurationProvider.Received().GetLogEnhancer(connector);
        _logger.Received().Record(batch, "Log.Provider", connector);
        await _partitionHandler.Received().NotifyEndOfPartition(batch, connector, taskId);
        _executionContext.Received().AddToCount(0);
        await _sinkProcessor.DidNotReceive().Process(batch, connector);
        await _sinkProcessor.DidNotReceive().Sink(batch, connector);
        _sinkExceptionHandler.DidNotReceive().Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        await _sinkExceptionHandler.DidNotReceive().HandleDeadLetter(batch, Arg.Any<Exception>(), connector);
        Assert.True(_sinkTask.IsStopped);
    }
    
    [Fact]
    public async void Execute_ProcessAndSinkInternalWhenSuccess()
    {
        const string connector = "connector";
        const int taskId = 1;
        var cts = GetCancellationToken();
        var batch = new SinkRecordBatch("") { new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>()}) };
        _sinkConsumer.Subscribe(Arg.Any<string>(), Arg.Any<int>()).Returns(_consumer);
        _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new BatchPollContext());
        _executionContext.GetNextPollIndex().Returns(10);
        _sinkConsumer.Consume(Arg.Any<IConsumer<byte[], byte[]>>(), Arg.Any<string>(), Arg.Any<int>()).Returns(batch);
        _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async d=> batch = await d.Invoke(batch)), Arg.Any<SinkRecordBatch>(), Arg.Any<string>()).Returns(batch);
        _configurationProvider.GetLogEnhancer(Arg.Any<string>()).Returns("Log.Provider");

        await _sinkTask.Execute(connector, taskId, cts);
        
        _executionContext.Received().Initialize(connector, taskId, _sinkTask);
        _sinkConsumer.Received().Subscribe(connector, taskId);
        _logger.DidNotReceive().Warning("Failed to create the consumer, exiting from the sink task.");
        _executionContext.Received().GetOrSetBatchContext(connector, taskId, cts.Token);
        _executionContext.Received().GetNextPollIndex();
        await _sinkConsumer.Received().Consume(_consumer, connector, taskId);
        await _retriableHandler.Received().Retry(Arg.Any<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(), batch, connector);
        _partitionHandler.Received().CommitOffsets(batch, _consumer);
        _configurationProvider.Received().GetLogEnhancer(connector);
        _logger.Received().Record(batch, "Log.Provider", connector);
        await _partitionHandler.Received().NotifyEndOfPartition(batch, connector, taskId);
        _executionContext.Received().AddToCount(1);
        await _sinkProcessor.Received().Process(batch, connector);
        await _sinkProcessor.Received().Sink(batch, connector);
        _sinkExceptionHandler.DidNotReceive().Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        await _sinkExceptionHandler.DidNotReceive().HandleDeadLetter(batch, Arg.Any<Exception>(), connector);
        Assert.True(_sinkTask.IsStopped);
    }
    
    [Theory]
    [InlineData(true, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    public async Task Execute_ProcessAndSinkInternalWhenProcessThrowsException(bool errorTolerated, bool isLastAttempt, bool expectedThrow)
    {
        const string connector = "connector";
        const int taskId = 1;
        var cts = GetCancellationToken();
        var batch = new SinkRecordBatch("") { new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>()}) };
        _sinkConsumer.Subscribe(Arg.Any<string>(), Arg.Any<int>()).Returns(_consumer);
        _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new BatchPollContext());
        _executionContext.GetNextPollIndex().Returns(10);
        _sinkConsumer.Consume(Arg.Any<IConsumer<byte[], byte[]>>(), Arg.Any<string>(), Arg.Any<int>()).Returns(batch);
        _retriableHandler
            .Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async d => batch = await d.Invoke(batch)),
                Arg.Any<SinkRecordBatch>(), Arg.Any<string>()).Returns(batch);
        _configurationProvider.GetLogEnhancer(Arg.Any<string>()).Returns("Log.Provider");
        _configurationProvider.IsErrorTolerated(Arg.Any<string>()).Returns(errorTolerated);
        _sinkProcessor.When(sp => sp.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<string>())).Throw<Exception>();
        batch.IsLastAttempt = isLastAttempt;

        await _sinkTask.Execute(connector, taskId, cts);
        
        _executionContext.Received().Initialize(connector, taskId, _sinkTask);
        _sinkConsumer.Received().Subscribe(connector, taskId);
        _logger.DidNotReceive().Warning("Failed to create the consumer, exiting from the sink task.");
        _executionContext.Received().GetOrSetBatchContext(connector, taskId, cts.Token);
        _executionContext.Received().GetNextPollIndex();
        await _sinkConsumer.Received().Consume(_consumer, connector, taskId);
        await _retriableHandler.Received().Retry(Arg.Any<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(), batch, connector);
        _partitionHandler.Received().CommitOffsets(batch, _consumer);
        _configurationProvider.Received().GetLogEnhancer(connector);
        _logger.Received().Record(batch, "Log.Provider", connector);
        await _partitionHandler.Received().NotifyEndOfPartition(batch, connector, taskId);
        _executionContext.Received().AddToCount(1);
        await _sinkProcessor.DidNotReceive().Sink(batch, connector);
        _sinkExceptionHandler.Received(expectedThrow ? 0 : 1).Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        await _sinkExceptionHandler.Received(expectedThrow ? 0 : 1).HandleDeadLetter(batch, Arg.Any<Exception>(), connector);
        await Assert.ThrowsAsync<Exception>(() => _sinkProcessor.Process(batch, connector));
        Assert.True(_sinkTask.IsStopped);
    }
    
    [Theory]
    [InlineData(true, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    public async Task Execute_ProcessAndSinkInternalWhenSinkThrowsException(bool errorTolerated, bool isLastAttempt, bool expectedThrow)
    {
        const string connector = "connector";
        const int taskId = 1;
        var cts = GetCancellationToken();
        var batch = new SinkRecordBatch("") { new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>()}) };
        _sinkConsumer.Subscribe(Arg.Any<string>(), Arg.Any<int>()).Returns(_consumer);
        _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new BatchPollContext());
        _executionContext.GetNextPollIndex().Returns(10);
        _sinkConsumer.Consume(Arg.Any<IConsumer<byte[], byte[]>>(), Arg.Any<string>(), Arg.Any<int>()).Returns(batch);
        _retriableHandler
            .Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async d => batch = await d.Invoke(batch)),
                Arg.Any<SinkRecordBatch>(), Arg.Any<string>()).Returns(batch);
        _configurationProvider.GetLogEnhancer(Arg.Any<string>()).Returns("Log.Provider");
        _configurationProvider.IsErrorTolerated(Arg.Any<string>()).Returns(errorTolerated);
        _sinkProcessor.When(sp => sp.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<string>())).Throw<Exception>();
        batch.IsLastAttempt = isLastAttempt;

        await _sinkTask.Execute(connector, taskId, cts);
        
        _executionContext.Received().Initialize(connector, taskId, _sinkTask);
        _sinkConsumer.Received().Subscribe(connector, taskId);
        _logger.DidNotReceive().Warning("Failed to create the consumer, exiting from the sink task.");
        _executionContext.Received().GetOrSetBatchContext(connector, taskId, cts.Token);
        _executionContext.Received().GetNextPollIndex();
        await _sinkConsumer.Received().Consume(_consumer, connector, taskId);
        await _retriableHandler.Received().Retry(Arg.Any<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(), batch, connector);
        _partitionHandler.Received().CommitOffsets(batch, _consumer);
        _configurationProvider.Received().GetLogEnhancer(connector);
        _logger.Received().Record(batch, "Log.Provider", connector);
        await _partitionHandler.Received().NotifyEndOfPartition(batch, connector, taskId);
        _executionContext.Received().AddToCount(1);
        await _sinkProcessor.Received().Process(batch, connector);
        _sinkExceptionHandler.Received(expectedThrow ? 0 : 1).Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        await _sinkExceptionHandler.Received(expectedThrow ? 0 : 1).HandleDeadLetter(batch, Arg.Any<Exception>(), connector);
        await Assert.ThrowsAsync<Exception>(() => _sinkProcessor.Sink(batch, connector));
        Assert.True(_sinkTask.IsStopped);
    }
    
    private CancellationTokenSource GetCancellationToken(int loop = 2, int delay = 0, Action assertBeforeCancel = null)
    {
        var cts = new CancellationTokenSource();
        _tokenHandler.When(k => k.DoNothing()).Do(_ =>
        {
            if (--loop == 0)
            {
                assertBeforeCancel?.Invoke();
                if(delay == 0) cts.Cancel();
                else cts.CancelAfter(delay);
            }
        });
        return cts;
    }
}


/*
public class SinkTaskTests
{
    private readonly IKafkaClientBuilder _kafkaClientBuilder;
    private readonly ISinkConsumer _sinkConsumer;
    private readonly ISinkProcessor _sinkProcessor;
    private readonly IPartitionHandler _partitionHandler;
    private readonly ISinkExceptionHandler _sinkExceptionHandler;
    private readonly IRetriableHandler _retriableHandler;
    private readonly SinkTask _sinkTask;
    private readonly IConsumer<byte[], byte[]> _consumer;

    public SinkTaskTests()
    {
        var logger = Substitute.For<ILogger<SinkTask>>();
        _kafkaClientBuilder = Substitute.For<IKafkaClientBuilder>();
        _sinkConsumer = Substitute.For<ISinkConsumer>();
        _sinkProcessor = Substitute.For<ISinkProcessor>();
        _partitionHandler = Substitute.For<IPartitionHandler>();
        _sinkExceptionHandler = Substitute.For<ISinkExceptionHandler>();
        _retriableHandler = Substitute.For<IRetriableHandler>();
        _consumer = Substitute.For<IConsumer<byte[], byte[]>>();
        _sinkTask = new SinkTask(logger, _kafkaClientBuilder, _sinkConsumer, _sinkProcessor,
            _partitionHandler, _sinkExceptionHandler, _retriableHandler, null)
        {
        };
    }

    [Fact]
    public async Task Start_When_MethodsCalledInSequence()
    {
        _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
        var batch = new SinkRecordBatch("") {new SinkRecord(null)};
        _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
        _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        await _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
            Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>());

        await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

        //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
        _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
        await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
        await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.Received().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.Received().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        _sinkExceptionHandler.DidNotReceive().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
            Arg.Any<Action>());
        _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>());
        await _partitionHandler.DidNotReceive()
            .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
    }

    [Fact]
    public async Task Start_When_ConsumeThrowsException()
    {
        _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
        var batch = new SinkRecordBatch("");
        _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Throws<Exception>();
        _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        await _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
            Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>());

        await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

        //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
        _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
        await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
        await _sinkProcessor.DidNotReceive().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        await _sinkProcessor.DidNotReceive().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.DidNotReceive().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.DidNotReceive().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        _sinkExceptionHandler.Received().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
            Arg.Any<Action>());
        _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>());
        await _partitionHandler.DidNotReceive()
            .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
    }

    [Fact]
    public async Task Start_When_ProcessThrowsException()
    {
        _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
        var batch = new SinkRecordBatch("") {new SinkRecord(null)};
        _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
        _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Throws<Exception>();
        _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
            Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>()).Throws<Exception>();

        await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

        //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
        _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
        await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
        await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        await _sinkProcessor.DidNotReceive().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.DidNotReceive().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.DidNotReceive().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        _sinkExceptionHandler.Received().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
            Arg.Any<Action>());
        _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>());
        await _partitionHandler.DidNotReceive()
            .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
    }

    [Fact]
    public async Task Start_When_SinkThrowsException()
    {
        _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
        var batch = new SinkRecordBatch("") {new SinkRecord(null)};
        _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
        _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Throws<Exception>();
        //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
            Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>()).Throws<Exception>();

        await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

        //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
        _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
        await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
        await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.DidNotReceive().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.DidNotReceive().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        _sinkExceptionHandler.Received().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
            Arg.Any<Action>());
        _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>());
        await _partitionHandler.DidNotReceive()
            .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
    }

    [Fact]
    public async Task Start_When_StreamerProcessThrowsException()
    {
        _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
        var batch = new SinkRecordBatch("") {new SinkRecord(null)};
        _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
        _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Throws<Exception>();
        //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
            Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>()).Throws<Exception>();

        await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

       //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
        _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
        await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
        await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.Received().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.DidNotReceive().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        _sinkExceptionHandler.Received().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
            Arg.Any<Action>());
        _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>());
        await _partitionHandler.DidNotReceive()
            .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
    }

    [Fact]
    public async Task Start_When_StreamerPublishThrowsException()
    {
        _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
        var batch = new SinkRecordBatch("") {new SinkRecord(null)};
        _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
        _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Throws<Exception>();
        _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
            Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>()).Throws<Exception>();

        await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

        //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
        _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
        await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
        await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.Received().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.Received().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        _sinkExceptionHandler.Received().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
            Arg.Any<Action>());
        _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>());
        await _partitionHandler.DidNotReceive()
            .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Start_When_BatchIsNullOrEmpty(bool isNull)
    {
        _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
        var batch = new SinkRecordBatch("unitTests") {new SinkRecord(null)};
        _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
        _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>())
           // .Returns(isNull ? null : batch);
        await _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
            Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>());

        await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

        //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
        _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
        await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
        await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.Received().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.Received().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        _sinkExceptionHandler.DidNotReceive().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
            Arg.Any<Action>());
        _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>());
        await _partitionHandler.DidNotReceive()
            .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
    }

    [Fact]
    public async Task Start_When_BatchIsNotEmpty()
    {
        _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
        var batch = new SinkRecordBatch("")
        {
            new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers(), Timestamp = new Timestamp(DateTime.Now)
                }
            }
        };
        _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
        _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
            Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>()).Returns(batch);

        await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

        //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
        _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
        await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
        await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.Received().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.Received().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        _sinkExceptionHandler.DidNotReceive().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
            Arg.Any<Action>());
        _partitionHandler.Received().CommitOffsets(Arg.Any<SinkRecordBatch>(), _consumer,
            Arg.Any<ConnectorConfig>());
        await _partitionHandler.Received().NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
    }

    [Fact]
    public async Task Start_When_ConsumerIsNull()
    {
        _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns((IConsumer<byte[], byte[]>) null);
        var batch = new SinkRecordBatch("");
        _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
        _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
        //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Throws<Exception>();
        await _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
            Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>());

        await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

        //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
        _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
        await _sinkConsumer.DidNotReceive().Consume(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
        await _sinkProcessor.DidNotReceive().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        await _sinkProcessor.DidNotReceive().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.DidNotReceive().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        //await _messageStreamer.DidNotReceive().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        _sinkExceptionHandler.DidNotReceive().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
            Arg.Any<Action>());
        _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
            Arg.Any<IConsumer<byte[], byte[]>>(),
            Arg.Any<ConnectorConfig>());
        await _partitionHandler.DidNotReceive()
            .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
    }
}
*/