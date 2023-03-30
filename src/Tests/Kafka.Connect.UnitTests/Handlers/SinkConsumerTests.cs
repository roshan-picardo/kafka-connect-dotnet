using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Handlers;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;
using Timestamp = Confluent.Kafka.Timestamp;

namespace Kafka.Connect.UnitTests.Handlers
{
    public class SinkConsumerTests
    {
        private readonly ILogger<SinkConsumer> _logger;
        private readonly IExecutionContext _executionContext;
        private readonly IRetriableHandler _retriableHandler;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IKafkaClientBuilder _kafkaClientBuilder;
        private readonly IConsumer<byte[], byte[]> _consumer;
        private readonly SinkConsumer _sinkConsumer;
      
        public SinkConsumerTests()
        {
            _logger = Substitute.For<ILogger<SinkConsumer>>();
            _executionContext = Substitute.For<IExecutionContext>();
            _retriableHandler = Substitute.For<IRetriableHandler>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _kafkaClientBuilder = Substitute.For<IKafkaClientBuilder>();
            _consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            _sinkConsumer = new SinkConsumer(_logger, _executionContext, _retriableHandler, _configurationProvider, _kafkaClientBuilder);
        }

        [Fact]
        public void Subscribe_WhenReturnsConsumer()
        {
            _configurationProvider.GetTopics(Arg.Any<string>()).Returns(new[] {"topic"});
            _kafkaClientBuilder.GetConsumer(Arg.Any<string>(), Arg.Any<int>()).Returns(_consumer);

            var actual = _sinkConsumer.Subscribe("connector", 1);
            
            Assert.Equal(_consumer, actual);
            _kafkaClientBuilder.Received().GetConsumer("connector", 1);
            _consumer.Received().Subscribe(Arg.Any<IList<string>>());
        }
        
        [Fact]
        public void Subscribe_ThrowsExceptionAtGetConsumer()
        {
            _configurationProvider.GetTopics(Arg.Any<string>()).Returns(new[] {"topic"});
            _kafkaClientBuilder.GetConsumer(Arg.Any<string>(), Arg.Any<int>()).Throws<Exception>();

            var actual = _sinkConsumer.Subscribe("connector", 1);
            
            Assert.Null(actual);
            _kafkaClientBuilder.Received().GetConsumer("connector", 1);
            _consumer.DidNotReceive().Subscribe(Arg.Any<IList<string>>());
            _logger.Received().Critical("Failed to establish the connection Kafka brokers.", Arg.Any<Exception>());
        }
        
        [Fact]
        public void Subscribe_ThrowsExceptionAtSubscribe()
        {
            _configurationProvider.GetTopics(Arg.Any<string>()).Returns(new[] {"topic"});
            _kafkaClientBuilder.GetConsumer(Arg.Any<string>(), Arg.Any<int>()).Returns(_consumer);
            _consumer.When(c=> c.Subscribe(Arg.Any<string[]>())).Throw<Exception>();

            var actual = _sinkConsumer.Subscribe("connector", 1);
            
            Assert.Null(actual);
            _kafkaClientBuilder.Received().GetConsumer("connector", 1);
            _consumer.Received().Subscribe(Arg.Any<IList<string>>());
            _logger.Received().Critical("Failed to establish the connection Kafka brokers.", Arg.Any<Exception>());
        }
        
        [Theory]
        [InlineData(null)]
        [InlineData(" ")]
        [InlineData()]
        public void Subscribe_WhenTopicsIsNullOrEmptyList(params string[] topics)
        {
            _configurationProvider.GetTopics(Arg.Any<string>()).Returns(topics);

            var actual = _sinkConsumer.Subscribe("connector", 1);
            
            Assert.Null(actual);
            _kafkaClientBuilder.DidNotReceive().GetConsumer("connector", 1);
            _consumer.DidNotReceive().Subscribe(Arg.Any<IList<string>>());
            _logger.Received().Warning("No topics to subscribe.");
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Consume_WhenConsumedIsNullOrEmpty(bool isNull)
        {
            var batch = isNull ?  null : new SinkRecordBatch("");

            _retriableHandler.Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(),  Arg.Any<string>())
                .Returns(await Task.FromResult(batch));

            var actual = await _sinkConsumer.Consume(_consumer, "connector", 1);
             
            Assert.Equal(actual, batch);
            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<string>());
            _consumer.DidNotReceive().Consume(Arg.Any<CancellationToken>());
            _logger.Debug("There aren't any messages in the batch to process.");
        }
        
        [Fact]
        public async Task Consume_WhenConsumedReturnsABatch()
        {
            var record1 = new SinkRecord(new ConsumeResult<byte[], byte[]>() { Message = new Message<byte[], byte[]>()});
            var record2 = new SinkRecord(new ConsumeResult<byte[], byte[]>() {Message = new Message<byte[], byte[]>()});
            var batch = new SinkRecordBatch("") {record1, record2};
            _retriableHandler.Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(),  Arg.Any<string>())
                .Returns(await Task.FromResult(batch));

            var actual = await _sinkConsumer.Consume(_consumer, "connector", 1);
             
            Assert.Equal(actual, batch);
            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<string>());
            Assert.NotNull(record1.Consumed.Message.Headers);
            Assert.NotNull(record2.Consumed.Message.Headers);
            Assert.Contains(record1.Consumed.Message.Headers, h => h.Key == "_logTimestamp");
            Assert.Contains(record2.Consumed.Message.Headers, h => h.Key == "_logTimestamp");
        }
        
        [Theory]
        [InlineData(-1, 100)]
        [InlineData(0, 100)]
        [InlineData(1, 1)]
        [InlineData(10, 10)]
        public async Task Consume_WhenBatchSizeSetDifferently(int size, int expectedCount)
        {
            var pollContext = new BatchPollContext {Iteration = 5};
            var timestamp = new Timestamp(new DateTime(2022, 06, 01, 12, 12, 12));
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = timestamp},
                Topic = "topic",
                Partition = 0,
                Offset = 0
            };
            _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>()).Returns(pollContext);
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Size = size});
            _consumer.Consume(Arg.Any<CancellationToken>()).Returns(consumed);
            Task<SinkRecordBatch> batch = null;
            await _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(b => batch =  b.Invoke()), Arg.Any<string>());

            await _sinkConsumer.Consume(_consumer, "connector", 1);
            var consumedBatch = await batch;
            
            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<string>());
            _consumer.Received(expectedCount).Consume(Arg.Any<CancellationToken>());
            Assert.Equal(expectedCount, consumedBatch.Count);
            _logger.Received().Trace("Polling for messages.", Arg.Any<object>());
            //_logger.Received().Log(LogLevel.Debug, Constants.AtLog, new {Message="Message consumed.", Topic = "topic", Partition = 0, Offset = 0, IsPartitionEOF = false, TimeStamp = timestamp.UtcDateTime });
        }
        
        [Fact]
        public async Task Consume_When2MessagesAreNullOutOf5Expected()
        {
            var pollContext = new BatchPollContext {Iteration = 5};
            var timestamp = new Timestamp(new DateTime(2022, 06, 01, 12, 12, 12));
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = timestamp},
                Topic = "topic",
                Partition = 0,
                Offset = 0
            };
            _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>()).Returns(pollContext);
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Size = 5});
            _consumer.Consume(Arg.Any<CancellationToken>()).Returns(consumed, null, consumed, consumed, null);
            Task<SinkRecordBatch> batch = null;
            await _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(b => batch =  b.Invoke()), Arg.Any<string>());

            await _sinkConsumer.Consume(_consumer, "connector", 1);
            var consumedBatch = await batch;
            
            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<string>());
            _consumer.Received(5).Consume(Arg.Any<CancellationToken>());
            Assert.Equal(3, consumedBatch.Count);
            _logger.Received().Trace("Polling for messages.", Arg.Any<object>());
            //_logger.Received().Log(LogLevel.Debug, Constants.AtLog, new {Message="Message consumed.", Topic = "topic", Partition = 0, Offset = 0, IsPartitionEOF = false, TimeStamp = timestamp.UtcDateTime });
        }
        
        [Fact]
        public async Task Consume_When3rdMessagesIsEOFOutOf5Expected()
        {
            var pollContext = new BatchPollContext {Iteration = 5};
            var timestamp = new Timestamp(new DateTime(2022, 06, 01, 12, 12, 12));
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = timestamp},
                Topic = "topic",
                Partition = 0,
                Offset = 0
            };
            var consumedEof = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = timestamp},
                Topic = "topic",
                Partition = 0,
                Offset = 3,
                IsPartitionEOF = true
            };
            _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>()).Returns(pollContext);
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Size = 5});
            _consumer.Consume(Arg.Any<CancellationToken>()).Returns(consumed, consumed, consumedEof, consumed, consumed);
            Task<SinkRecordBatch> batch = null;
            await _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(b => batch =  b.Invoke()), Arg.Any<string>());

            await _sinkConsumer.Consume(_consumer, "connector", 1);
            var consumedBatch = await batch;
            
            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<string>());
            _consumer.Received(3).Consume(Arg.Any<CancellationToken>());
            Assert.Equal(2, consumedBatch.Count);
            Assert.Single(consumedBatch.GetEofPartitions());
            Assert.Contains(consumedBatch.GetEofPartitions(), tpo => tpo.Topic == "topic" && tpo.Partition == 0 && tpo.Offset == 3);
            _logger.Received().Trace("Polling for messages.", Arg.Any<object>());
            //_logger.Received().Log(LogLevel.Debug, Constants.AtLog, new {Message="Message consumed.", Topic = "topic", Partition = 0, Offset = 0, IsPartitionEOF = false, TimeStamp = timestamp.UtcDateTime });
        }
        
        [Fact]
        public async Task Consume_WhenExceptionThrownAfter2Messages()
        {
            var pollContext = new BatchPollContext {Iteration = 5};
            var timestamp = new Timestamp(new DateTime(2022, 06, 01, 12, 12, 12));
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = timestamp},
                Topic = "topic",
                Partition = 0,
                Offset = 0
            };
            _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>()).Returns(pollContext);
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Size = 5});
            _consumer.Consume(Arg.Any<CancellationToken>()).Returns(_=> consumed, _=>  consumed, _=> throw new Exception());
            Task<SinkRecordBatch> batch = null;
            await _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(b => batch =  b.Invoke()), Arg.Any<string>());

            await _sinkConsumer.Consume(_consumer, "connector", 1);
            var consumedBatch = await batch;
            
            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<string>());
            _consumer.Received(3).Consume(Arg.Any<CancellationToken>());
            Assert.Equal(2, consumedBatch.Count);
            _logger.Received().Trace("Polling for messages.", Arg.Any<object>());
            _logger.Received().Warning("Consume failed. Part of the batch will be processed.", Arg.Any<object>(),
                Arg.Any<Exception>());
            //_logger.Received().Log(LogLevel.Debug, Constants.AtLog, new {Message="Message consumed.", Topic = "topic", Partition = 0, Offset = 0, IsPartitionEOF = false, TimeStamp = timestamp.UtcDateTime });
        }
        
        [Fact]
        public async Task Consume_WhenConsumeThrowsOperationCanceledException()
        {
            var pollContext = new BatchPollContext {Iteration = 5};
            _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>()).Returns(pollContext);
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Size = 5});
            _consumer.Consume(Arg.Any<CancellationToken>()).Throws<OperationCanceledException>();
            Task<SinkRecordBatch> batch = null;
            await _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(b => batch =  b.Invoke()), Arg.Any<string>());

            await _sinkConsumer.Consume(_consumer, "connector", 1);
            var consumedBatch = await batch;
            
            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<string>());
            _consumer.Received(1).Consume(Arg.Any<CancellationToken>());
            Assert.Empty(consumedBatch);
            _logger.Received().Trace("Polling for messages.", Arg.Any<object>());
            //_logger.Received().Trace("Task has been cancelled. The consume operation will be terminated.", Arg.Any<object>);
            //_logger.Received().Log(LogLevel.Debug, Constants.AtLog, new {Message="Message consumed.", Topic = "topic", Partition = 0, Offset = 0, IsPartitionEOF = false, TimeStamp = timestamp.UtcDateTime });
        }
        
        [Fact]
        public async Task Consume_WhenConsumeThrowsConsumerException()
        {
            var pollContext = new BatchPollContext {Iteration = 5};
            var timestamp = new Timestamp(new DateTime(2022, 06, 01, 12, 12, 12));
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = timestamp},
                Topic = "topic",
                Partition = 0,
                Offset = 0
            };
            _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>()).Returns(pollContext);
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Size = 5});
            _consumer.Consume(Arg.Any<CancellationToken>()).Throws( _=> new ConsumeException(consumed, ErrorCode.Unknown));
            Task<SinkRecordBatch> batch = null;
            await _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(b => batch =  b.Invoke()), Arg.Any<string>());

            await _sinkConsumer.Consume(_consumer, "connector", 1);
            await Assert.ThrowsAsync<ConnectRetriableException>( () =>  batch);
            
            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<string>());
            _consumer.Received(1).Consume(Arg.Any<CancellationToken>());
            _logger.Received().Trace("Polling for messages.", Arg.Any<object>());
            //_logger.Received().Log(LogLevel.Debug, Constants.AtLog, new {Message="Message consumed.", Topic = "topic", Partition = 0, Offset = 0, IsPartitionEOF = false, TimeStamp = timestamp.UtcDateTime });
        }
        
        [Fact]
        public async Task Consume_WhenConsumeThrowsGenericException()
        {
            var pollContext = new BatchPollContext {Iteration = 5};
            _executionContext.GetOrSetBatchContext(Arg.Any<string>(), Arg.Any<int>()).Returns(pollContext);
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Size = 5});
            _consumer.Consume(Arg.Any<CancellationToken>()).Throws<Exception>();
            Task<SinkRecordBatch> batch = null;
            await _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(b => batch =  b.Invoke()), Arg.Any<string>());

            await _sinkConsumer.Consume(_consumer, "connector", 1);
            await Assert.ThrowsAsync<ConnectDataException>( () =>  batch);
            
            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<string>());
            _consumer.Received(1).Consume(Arg.Any<CancellationToken>());
            _logger.Received().Trace("Polling for messages.", Arg.Any<object>());
            //_logger.Received().Log(LogLevel.Debug, Constants.AtLog, new {Message="Message consumed.", Topic = "topic", Partition = 0, Offset = 0, IsPartitionEOF = false, TimeStamp = timestamp.UtcDateTime });
        }
    }
}