using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Config;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Handlers;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;
using Timestamp = Confluent.Kafka.Timestamp;

namespace Kafka.Connect.Tests.Handlers
{
    public class SinkConsumerTests
    {
        private readonly IKafkaClientBuilder _kafkaClientBuilder;
        private readonly IRetriableHandler _retriableHandler;
        private readonly SinkConsumer _sinkConsumer;

        public SinkConsumerTests()
        {
            _kafkaClientBuilder = Substitute.For<IKafkaClientBuilder>();
            _retriableHandler = Substitute.For<IRetriableHandler>();
            var logger = Substitute.For<ILogger<SinkConsumer>>();
            _sinkConsumer = new SinkConsumer(logger, _kafkaClientBuilder, _retriableHandler);
        }

        [Fact]
        public void Subscribe_When_ReturnsConsumer()
        {
            var config = new ConnectorConfig
            {
                BootstrapServers = "broker:1100",
                Topics = new[] {"topicA"}
            };
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            _kafkaClientBuilder.GetConsumer(Arg.Any<ConnectorConfig>()).Returns(consumer);

            var actual = _sinkConsumer.Subscribe(config);
            
            Assert.Equal(consumer, actual);
            _kafkaClientBuilder.Received().GetConsumer(Arg.Any<ConnectorConfig>());
            consumer.Received().Subscribe(Arg.Any<IList<string>>());
        }
        
        [Fact]
        public void Subscribe_When_ExceptionAtGetConsumer()
        {
            var config = new ConnectorConfig
            {
                BootstrapServers = "broker:1100",
                Topics = new[] {"topicA"}
            };
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            _kafkaClientBuilder.GetConsumer(Arg.Any<ConnectorConfig>()).Throws<Exception>();

            var actual = _sinkConsumer.Subscribe(config);
            
            Assert.Null(actual);
            _kafkaClientBuilder.Received().GetConsumer(Arg.Any<ConnectorConfig>());
            consumer.DidNotReceive().Subscribe(Arg.Any<IList<string>>());
        }
        
        [Fact]
        public void Subscribe_When_ExceptionAtSubscribe()
        {
            var config = new ConnectorConfig
            {
                BootstrapServers = "broker:1100",
                Topics = new[] {"topicA"}
            };
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            _kafkaClientBuilder.GetConsumer(Arg.Any<ConnectorConfig>()).Returns(consumer);
            consumer.When(c => c.Subscribe(Arg.Any<IList<string>>())).Throw<Exception>();

            var actual = _sinkConsumer.Subscribe(config);
            
            Assert.Null(actual);
            _kafkaClientBuilder.Received().GetConsumer(Arg.Any<ConnectorConfig>());
            consumer.Received().Subscribe(Arg.Any<IList<string>>());
        }

        
        [Theory]
        [InlineData(true, null, null)]
        [InlineData(false, null, null)]
        [InlineData(false, "", null)]
        [InlineData(false, "broker:111", null)]
        [InlineData(false, "broker:111", "")]
        [InlineData(false, "broker:111", "  ")]
        public void Subscribe_When_ConfigsAreNullOrEmpty(bool isNull, string brokers, string topics)
        {
            var config = isNull
                ? null
                : new ConnectorConfig()
                {
                    BootstrapServers = brokers,
                    Topics = topics == null ? null :
                        topics == string.Empty ? new List<string>() : new List<string> {topics}
                };
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            _kafkaClientBuilder.GetConsumer(Arg.Any<ConnectorConfig>()).Returns(consumer);
            
            var actual = _sinkConsumer.Subscribe(config);
            
            Assert.Null(actual);
            _kafkaClientBuilder.DidNotReceive().GetConsumer(Arg.Any<ConnectorConfig>());
            consumer.DidNotReceive().Subscribe(Arg.Any<IList<string>>());
        }


        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Consume_When_ConsumedIsNullOrEmpty(bool isNull)
        {
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            var batch = new SinkRecordBatch("unit-tests");
            var config = new ConnectorConfig();
            var pollContext = new BatchPollContext();
            
             _retriableHandler.Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<int>(), Arg.Any<int>())
                .Returns(await Task.FromResult(isNull ?  null : new SinkRecordBatch("")));

             var actual = await _sinkConsumer.Consume(batch, consumer, config, pollContext);
             
             Assert.Equal(actual, batch);
             await _retriableHandler.Received()
                 .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<int>(), Arg.Any<int>());
             consumer.DidNotReceive().Consume(Arg.Any<CancellationToken>());
        }

        [Theory]
        [InlineData(false, false, -1, 1)]
        [InlineData(false, false, 0, 1)]
        [InlineData(false, false, 1, 1)]
        [InlineData(false, false, 10, 1)]
        [InlineData(true, false, -1, 1)]
        [InlineData(true, false, 0, 1)]
        [InlineData(true, false, 1, 1)]
        [InlineData(true, false, 10, 10)]
        [InlineData(false, true, 0, 1)]
        [InlineData(true, true, 0, 1)]
        public async Task Consume_When_BatchSizeSetDifferently(bool eof, bool batchIsNull, int size,
            int expectedCount)
        {
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            var batch = new SinkRecordBatch("unit-tests");
            var config = new ConnectorConfig
            {
                EnablePartitionEof = eof,
                Batch = batchIsNull ? null : new BatchConfig {Size = size}
            };
            var pollContext = new BatchPollContext();
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = new Timestamp(DateTime.Now)}
            };
            consumer.Consume(Arg.Any<CancellationToken>()).Returns(consumed);

            SinkRecordBatch consumedBatch = null;
            _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(async b => consumedBatch = await b.Invoke()),
                Arg.Any<int>(), Arg.Any<int>()).Returns(consumedBatch);
            consumer.Consume(Arg.Any<CancellationToken>());

            await _sinkConsumer.Consume(batch, consumer, config, pollContext);

            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<int>(), Arg.Any<int>());
            consumer.Received(expectedCount).Consume(Arg.Any<CancellationToken>());
            Assert.Equal(expectedCount, consumedBatch.Count);
        }
        
        [Fact]
        public async Task Consume_When_Of5ConsumedOneIsNull()
        {
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            var batch = new SinkRecordBatch("unit-tests");
            var config = new ConnectorConfig
            {
                EnablePartitionEof = true,
                Batch = new BatchConfig(){Size = 5}
            };
            var pollContext = new BatchPollContext();
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = new Timestamp(DateTime.Now)}
            };
            consumer.Consume(Arg.Any<CancellationToken>()).Returns(consumed, consumed, null, consumed, consumed);

            SinkRecordBatch consumedBatch = null;
            _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(async b => consumedBatch = await b.Invoke()),
                Arg.Any<int>(), Arg.Any<int>()).Returns(consumedBatch);
            consumer.Consume(Arg.Any<CancellationToken>());

            await _sinkConsumer.Consume(batch, consumer, config, pollContext);

            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<int>(), Arg.Any<int>());
            consumer.Received(5).Consume(Arg.Any<CancellationToken>());
            Assert.Equal(4, consumedBatch.Count);
        }
        
        [Fact]
        public async Task Consume_When_OfConsumedOneIsEOF()
        {
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            var batch = new SinkRecordBatch("unit-tests");
            var config = new ConnectorConfig
            {
                EnablePartitionEof = true,
                Batch = new BatchConfig(){Size = 5}
            };
            var pollContext = new BatchPollContext();
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = new Timestamp(DateTime.Now)}
            };
            var consumedEof = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = new Timestamp(DateTime.Now)},
                IsPartitionEOF = true,
                TopicPartitionOffset = new TopicPartitionOffset("TopicA", new Partition(1), new Offset(100))
            };
            consumer.Consume(Arg.Any<CancellationToken>()).Returns(consumed, consumed, consumedEof, consumed, consumed);

            SinkRecordBatch consumedBatch = null;
            _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(async b => consumedBatch = await b.Invoke()),
                Arg.Any<int>(), Arg.Any<int>()).Returns(consumedBatch);
            consumer.Consume(Arg.Any<CancellationToken>());

            await _sinkConsumer.Consume(batch, consumer, config, pollContext);

            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<int>(), Arg.Any<int>());
            consumer.Received(3).Consume(Arg.Any<CancellationToken>());
            Assert.Equal(2, consumedBatch.Count);
            Assert.NotNull(consumedBatch.GetEofPartitions().SingleOrDefault(tpo => tpo.Offset == 100));
        }
        
        [Fact]
        public async Task Consume_When_ConsumeThrowsExceptionAfter2Messages()
        {
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            var batch = new SinkRecordBatch("unit-tests");
            var config = new ConnectorConfig
            {
                EnablePartitionEof = true,
                Batch = new BatchConfig {Size = 5}
            };
            var pollContext = new BatchPollContext();
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = new Timestamp(DateTime.Now)}
            };

            consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(c => consumed, c => consumed, c => throw new Exception());

            SinkRecordBatch consumedBatch = null;
            _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(async b => consumedBatch = await b.Invoke()),
                Arg.Any<int>(), Arg.Any<int>()).Returns(consumedBatch);

            await _sinkConsumer.Consume(batch, consumer, config, pollContext);

            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<int>(), Arg.Any<int>());
            consumer.Received(3).Consume(Arg.Any<CancellationToken>());
            Assert.Equal(2, consumedBatch.Count);
        }
        
        [Fact]
        public async Task Consume_When_ConsumeThrowsOperationCanceledException()
        {
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            var batch = new SinkRecordBatch("unit-tests");
            var config = new ConnectorConfig
            {
                EnablePartitionEof = true,
                Batch = new BatchConfig {Size = 5}
            };
            var pollContext = new BatchPollContext();
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = new Timestamp(DateTime.Now)}
            };

            consumer.Consume(Arg.Any<CancellationToken>()).Throws<OperationCanceledException>();

            SinkRecordBatch consumedBatch = null;
            _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(async b => consumedBatch = await b.Invoke()),
                Arg.Any<int>(), Arg.Any<int>()).Returns(consumedBatch);

            await _sinkConsumer.Consume(batch, consumer, config, pollContext);

            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<int>(), Arg.Any<int>());
            consumer.Received(1).Consume(Arg.Any<CancellationToken>());
            Assert.Empty(consumedBatch);
        }
        
        [Fact]
        public async Task Consume_When_ConsumeThrowsConsumeException()
        {
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            var batch = new SinkRecordBatch("unit-tests");
            var config = new ConnectorConfig
            {
                EnablePartitionEof = true,
                Batch = new BatchConfig {Size = 5}
            };
            var pollContext = new BatchPollContext();
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    {Headers = new Headers(), Timestamp = new Timestamp(DateTime.Now)}
            };

            consumer.Consume(Arg.Any<CancellationToken>())
                .Throws(c => throw new ConsumeException(consumed, new Error(ErrorCode.Unknown)));

            SinkRecordBatch consumedBatch = null;
            _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(async b =>
                    await Assert.ThrowsAsync<ConnectRetriableException>(async () => consumedBatch = await b.Invoke())),
                Arg.Any<int>(), Arg.Any<int>()).Returns(consumedBatch);

            await _sinkConsumer.Consume(batch, consumer, config, pollContext);

            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<int>(), Arg.Any<int>());
            consumer.Received(1).Consume(Arg.Any<CancellationToken>());
            Assert.Null(consumedBatch);
        }
        
        [Fact]
        public async Task Consume_When_ConsumeThrowsAnyException()
        {
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            var batch = new SinkRecordBatch("unit-tests");
            var config = new ConnectorConfig
            {
                EnablePartitionEof = true,
                Batch = new BatchConfig {Size = 5}
            };
            var pollContext = new BatchPollContext();

            consumer.Consume(Arg.Any<CancellationToken>()).Throws<Exception>();

            SinkRecordBatch consumedBatch = null;
            _retriableHandler.Retry(
                Arg.Do<Func<Task<SinkRecordBatch>>>(async b =>
                    await Assert.ThrowsAsync<ConnectDataException>(async () => consumedBatch = await b.Invoke())),
                Arg.Any<int>(), Arg.Any<int>()).Returns(consumedBatch);

            await _sinkConsumer.Consume(batch, consumer, config, pollContext);

            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<int>(), Arg.Any<int>());
            consumer.Received(1).Consume(Arg.Any<CancellationToken>());
            Assert.Null(consumedBatch);
        }
        
        [Fact]
        public async Task Consume_When_RetryHandlerReturnsBatch()
        {
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            var batch = new SinkRecordBatch("unit-tests");
            var config = new ConnectorConfig
            {
                EnablePartitionEof = true,
                Batch = new BatchConfig {Size = 5}
            };
            var pollContext = new BatchPollContext();
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                    { Timestamp = new Timestamp(DateTime.Now.AddSeconds(-1))}
            };

            var consumedBatch = new SinkRecordBatch("consumed") { consumed };
            _retriableHandler.Retry(
                Arg.Any<Func<Task<SinkRecordBatch>>>(),
                Arg.Any<int>(), Arg.Any<int>()).Returns(consumedBatch);

            await _sinkConsumer.Consume(batch, consumer, config, pollContext);

            await _retriableHandler.Received()
                .Retry(Arg.Any<Func<Task<SinkRecordBatch>>>(), Arg.Any<int>(), Arg.Any<int>());
            Assert.NotNull(batch);
            Assert.Equal(batch.First(), consumedBatch.First());
            Assert.NotNull(batch.First().Consumed.Message.Headers.SingleOrDefault(h=>h.Key == "_logTimestamp"));
        }
    }
}