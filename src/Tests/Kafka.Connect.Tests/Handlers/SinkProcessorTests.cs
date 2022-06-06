using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Serializers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Kafka.Connect.Tests.Handlers
{
    public class SinkProcessorTests
    {
        private readonly ILogger<SinkProcessor> _logger;
        private readonly IMessageConverter _messageConverter;
        private readonly IMessageHandler _messageHandler;
        private readonly ISinkHandlerProvider _sinkHandlerProvider;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly ISinkHandler _sinkHandler;

        private readonly SinkProcessor _sinkProcessor;

        public SinkProcessorTests()
        {
            _logger = Substitute.For<MockLogger<SinkProcessor>>();
            _messageConverter = Substitute.For<IMessageConverter>();
            _messageHandler = Substitute.For<IMessageHandler>();
            _sinkHandlerProvider = Substitute.For<ISinkHandlerProvider>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _sinkHandler = Substitute.For<ISinkHandler>();

            _sinkProcessor = new SinkProcessor(_logger, _messageConverter, _messageHandler, _sinkHandlerProvider, _configurationProvider);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Process_WhenBatchIsNullOrEmpty(bool isNull)
        {
            var batch = isNull ? null : new SinkRecordBatch("connector");

            await _sinkProcessor.Process(batch, "connector");

            await _messageConverter.DidNotReceive().Deserialize(Arg.Any<ConsumeResult<byte[], byte[]>>(), Arg.Any<string>());
            await _messageHandler.DidNotReceive().Process(Arg.Any<SinkRecord>(), Arg.Any<string>());
        }

        [Fact]
        public async Task Process_BatchContainingSingleMessage()
        {
            var batch = GetBatch();
            var record = batch.First();
            var key = new JObject{{"keyData", new JObject()}};
            var value = new JObject{{"valueData", new JObject()}};
            var data = new JObject
            {
                {"key", key},
                {"value", value}
            };

            _messageConverter.Deserialize(Arg.Any<ConsumeResult<byte[], byte[]>>(), Arg.Any<string>()).Returns((key, value));
            _messageHandler.Process(Arg.Any<SinkRecord>(), Arg.Any<string>()).Returns((true, data));
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Parallelism = 1});

            await _sinkProcessor.Process(batch, "connector");
            
            Assert.True(record.Skip);
            Assert.Equal(data, record.Data);
            Assert.Equal(SinkStatus.Processed, record.Status);
            await _messageConverter.Received().Deserialize(Arg.Any<ConsumeResult<byte[], byte[]>>(), Arg.Any<string>());
            await _messageHandler.Received().Process(Arg.Any<SinkRecord>(), Arg.Any<string>());
        }

        [Fact]
        public async Task Process_DeserializeThrowsException()
        {
            var batch = GetBatch(1, ("exception-topic", 10));
            var record = batch.First();
            var ce = new ConnectException();
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Parallelism = 1});
            _messageConverter.Deserialize(Arg.Any<ConsumeResult<byte[], byte[]>>(), Arg.Any<string>()).Throws(ce);
            
            await Assert.ThrowsAsync<ConnectAggregateException>(()=> _sinkProcessor.Process(batch, "connector"));
            
            Assert.Equal(SinkStatus.Failed, record.Status);
            Assert.Equal(record.Topic, ce.Topic);
            Assert.Equal(record.Partition, ce.Partition);
            Assert.Equal(record.Offset, ce.Offset);
            await _messageConverter.Received().Deserialize(Arg.Any<ConsumeResult<byte[], byte[]>>(), Arg.Any<string>());
            await _messageHandler.DidNotReceive().Process(Arg.Any<SinkRecord>(), Arg.Any<string>());
        }
        
        [Fact]
        public async Task Process_HandlerProcessThrowsException()
        {
            var batch = GetBatch(1, ("exception-topic", 10));
            var record = batch.First();
            var ce = new ConnectException();
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Parallelism = 1});
            _messageHandler.Process(Arg.Any<SinkRecord>(), Arg.Any<string>()).Throws(ce);
            
            await Assert.ThrowsAsync<ConnectAggregateException>(()=> _sinkProcessor.Process(batch, "connector"));
            
            Assert.Equal(SinkStatus.Failed, record.Status);
            Assert.Equal(record.Topic, ce.Topic);
            Assert.Equal(record.Partition, ce.Partition);
            Assert.Equal(record.Offset, ce.Offset);
            await _messageConverter.Received().Deserialize(Arg.Any<ConsumeResult<byte[], byte[]>>(), Arg.Any<string>());
            await _messageHandler.Received().Process(Arg.Any<SinkRecord>(), Arg.Any<string>());
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Sink_WhenBatchIsNullOrEmpty(bool isNull)
        {
            var batch = isNull ? null : new SinkRecordBatch("connector");

            await _sinkProcessor.Sink(batch, "connector");

            _sinkHandlerProvider.DidNotReceive().GetSinkHandler(Arg.Any<string>());
            await _sinkHandler.DidNotReceive().Put(Arg.Any<SinkRecordBatch>());
            _logger.DidNotReceive().Log(LogLevel.Warning, "{@Log}", new {Message = "Sink handler is not specified. Check if the handler is configured properly, and restart the connector."});
        } 
        
        [Fact]
        public async Task Sink_WhenNoHandlerIsSpecified()
        {
            var batch = GetBatch();
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns((ISinkHandler) null);

            await _sinkProcessor.Sink(batch, "connector");

            _sinkHandlerProvider.Received().GetSinkHandler(Arg.Any<string>());
            await _sinkHandler.DidNotReceive().Put(Arg.Any<SinkRecordBatch>());
            _logger.Received().Log(LogLevel.Warning, "{@Log}", new {Message = "Sink handler is not specified. Check if the handler is configured properly, and restart the connector."});
            Assert.True(batch.All(r=>r.Skip));
            Assert.True(batch.All(r=>r.Status == SinkStatus.Skipped));
        } 
        
        [Fact]
        public async Task Sink_WhenHasAValidHandler()
        {
            var batch = GetBatch();
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);

            await _sinkProcessor.Sink(batch, "connector");

            _sinkHandlerProvider.Received().GetSinkHandler(Arg.Any<string>());
            await _sinkHandler.Received().Put(Arg.Any<SinkRecordBatch>());
            _logger.DidNotReceive().Log(LogLevel.Warning, "{@Log}", new {Message = "Sink handler is not specified. Check if the handler is configured properly, and restart the connector."});
        } 
        
        private static SinkRecordBatch GetBatch(int length = 1, params (string topic, int partition)[] topicPartitions)
        {
            var batch = new SinkRecordBatch("connector");

            for (var i = 0; i < length; i++)
            {
                var (topic, partition) = topicPartitions != null && topicPartitions.Length > i ? topicPartitions[i] : ("topic", 0);
                batch.Add(new SinkRecord(new ConsumeResult<byte[], byte[]>
                    {Topic = topic, Partition = partition, Message = new Message<byte[], byte[]>() {Headers = new Headers()}}));
            }

            return batch;
        }
        
        /*
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task Process_When_SerializeAndProcessInvoked(bool processorsAreNull)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>() {Headers = new Headers()}, Topic = "TopicA",
            })
            {
                PublishMessages =
                    new Dictionary<string, Message<byte[], byte[]>>() {{"target-topic", new Message<byte[], byte[]>()}}
            };
            var config = new ConnectorConfig
            {
                Batch = new BatchConfig
                {
                    Parallelism = 1
                },
                Processors = processorsAreNull ? null : new List<ProcessorConfig>()
            };
            var batch = new SinkRecordBatch("unit-tests") {sinkRecord};
            SinkRecordBatch processedBatch = null;
            _messageConverter.Deserialize(Arg.Any<ConverterConfig>(), Arg.Any<ConsumeResult<byte[], byte[]>>())
                .Returns((new JObject(), new JObject()));
            _messageHandler.Process(Arg.Any<SinkRecord>(), Arg.Any<ConnectorConfig>())
                .Returns((true, new JObject()));

            var actual = await _sinkProcessor.Process(batch, config);

            await _messageConverter.Received()
                .Deserialize(Arg.Any<ConverterConfig>(), Arg.Any<ConsumeResult<byte[], byte[]>>());
            await _messageHandler.Received().Process(Arg.Any<SinkRecord>(), Arg.Any<ConnectorConfig>());
            Assert.Equal(SinkStatus.Processed, sinkRecord.Status);
        }
        
        [Theory]
        [InlineData(true, SinkStatus.Skipped)]
        [InlineData(false, SinkStatus.Processed)]
        public async Task Sink_When_PutIsInvoked(bool nullHandler, SinkStatus expected)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>() {Headers = new Headers()}, Topic = "TopicA",
            })
            {
                Status = SinkStatus.Processed
            };
            var config = new ConnectorConfig
            {
                Batch = new BatchConfig
                {
                    Parallelism = 1
                }
            };
            var batch = new SinkRecordBatch("unit-tests") {sinkRecord};
            var sinkHandler = Substitute.For<ISinkHandler>();
            SinkRecordBatch processedBatch = null;
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>(), Arg.Any<string>())
                .Returns(nullHandler ? null : sinkHandler);
            sinkHandler.Put(Arg.Any<SinkRecordBatch>()).Returns(batch);
            
            var actual = await _sinkProcessor.Sink(batch, config);

            _sinkHandlerProvider.Received().GetSinkHandler(Arg.Any<string>(), Arg.Any<string>());
            await sinkHandler.Received(nullHandler ? 0 : 1).Put(Arg.Any<SinkRecordBatch>());
            Assert.Equal(expected, sinkRecord.Status);
        }
        */
    }
}