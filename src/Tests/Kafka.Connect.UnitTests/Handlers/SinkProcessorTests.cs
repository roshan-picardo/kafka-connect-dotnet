using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace UnitTests.Kafka.Connect.Handlers
{
    public class SinkProcessorTests
    {
        private readonly ILogger<SinkProcessor> _logger;
        private readonly IMessageHandler _messageHandler;
        private readonly ISinkHandlerProvider _sinkHandlerProvider;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly ISinkHandler _sinkHandler;

        private readonly SinkProcessor _sinkProcessor;

        public SinkProcessorTests()
        {
            _logger = Substitute.For<ILogger<SinkProcessor>>();
            Substitute.For<IMessageConverter>();
            _messageHandler = Substitute.For<IMessageHandler>();
            _sinkHandlerProvider = Substitute.For<ISinkHandlerProvider>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _sinkHandler = Substitute.For<ISinkHandler>();
            _sinkProcessor = new SinkProcessor(_logger, _messageHandler, _sinkHandlerProvider, _configurationProvider);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Process_WhenBatchIsNullOrEmpty(bool isNull)
        {
            var batch = isNull ? null : new ConnectRecordBatch("connector");

            await _sinkProcessor.Process(batch, "connector");

            await _messageHandler.DidNotReceive().Deserialize(Arg.Any<string>(), Arg.Any<string>(),Arg.Any<ConnectMessage<byte[]>>());
            await _messageHandler.DidNotReceive().Process(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ConnectMessage<JsonNode>>());
        }

        [Fact]
        public async Task Process_BatchContainingSingleMessage()
        {
            var batch = GetBatch();
            var record = batch.First();
            var key = new JsonObject{{"keyData", new JsonObject()}};
            var value = new JsonObject{{"valueData", new JsonObject()}};
            var data = new ConnectMessage<JsonNode>
            {
                Key = key,
                Value = value
            };

            _messageHandler.Deserialize(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ConnectMessage<byte[]>>()).Returns(new ConnectMessage<JsonNode> { Key = key, Value = value});
            _messageHandler.Process(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ConnectMessage<JsonNode>>()).Returns((true, data));
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Parallelism = 1});

            await _sinkProcessor.Process(batch, "connector");
            
            Assert.True(record.Skip);
            Assert.Equal(data, record.Deserialized);
            Assert.Equal(SinkStatus.Processed, record.Status);
            await _messageHandler.Received().Deserialize(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ConnectMessage<byte[]>>());
            await _messageHandler.Received().Process(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ConnectMessage<JsonNode>>());
        }

        [Fact]
        public async Task Process_DeserializeThrowsException()
        {
            var batch = GetBatch(1, ("exception-topic", 10));
            var record = batch.First();
            var ce = new ConnectException();
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Parallelism = 1});
            _messageHandler.Deserialize(Arg.Any<string>(), Arg.Any<string>(),Arg.Any<ConnectMessage<byte[]>>()).Throws(ce);
            
            await Assert.ThrowsAsync<ConnectAggregateException>(()=> _sinkProcessor.Process(batch, "connector"));
            
            Assert.Equal(SinkStatus.Failed, record.Status);
            Assert.Equal(record.Topic, ce.Topic);
            Assert.Equal(record.Partition, ce.Partition);
            Assert.Equal(record.Offset, ce.Offset);
            await _messageHandler.Received().Deserialize(Arg.Any<string>(), Arg.Any<string>(),Arg.Any<ConnectMessage<byte[]>>());
            await _messageHandler.DidNotReceive().Process(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ConnectMessage<JsonNode>>());
        }
        
        [Fact]
        public async Task Process_HandlerProcessThrowsException()
        {
            var batch = GetBatch(1, ("exception-topic", 10));
            var record = batch.First();
            var ce = new ConnectException();
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig {Parallelism = 1});
            _messageHandler.Deserialize(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ConnectMessage<byte[]>>())
                .Returns(new ConnectMessage<JsonNode>());
            _messageHandler.Process(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ConnectMessage<JsonNode>>()).Throws(ce);
            
            await Assert.ThrowsAsync<ConnectAggregateException>(()=> _sinkProcessor.Process(batch, "connector"));
            
            Assert.Equal(SinkStatus.Failed, record.Status);
            Assert.Equal(record.Topic, ce.Topic);
            Assert.Equal(record.Partition, ce.Partition);
            Assert.Equal(record.Offset, ce.Offset);
            await _messageHandler.Received().Deserialize(Arg.Any<string>(), Arg.Any<string>(),Arg.Any<ConnectMessage<byte[]>>());
            await _messageHandler.Received().Process(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ConnectMessage<JsonNode>>());
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Sink_WhenBatchIsNullOrEmpty(bool isNull)
        {
            var batch = isNull ? null : new ConnectRecordBatch("connector");

            await _sinkProcessor.Sink(batch, "connector", 0);

            _sinkHandlerProvider.DidNotReceive().GetSinkHandler(Arg.Any<string>());
            await _sinkHandler.DidNotReceive().Put(Arg.Any<ConnectRecordBatch>(), Arg.Any<string>(), Arg.Any<int>());
            _logger.DidNotReceive().Warning("Sink handler is not specified. Check if the handler is configured properly, and restart the connector.");
        } 
        
        [Fact]
        public async Task Sink_WhenNoHandlerIsSpecified()
        {
            var batch = GetBatch();
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns((ISinkHandler) null);

            await _sinkProcessor.Sink(batch, "connector", 0);

            _sinkHandlerProvider.Received().GetSinkHandler(Arg.Any<string>());
            await _sinkHandler.DidNotReceive().Put(Arg.Any<ConnectRecordBatch>(),Arg.Any<string>(), Arg.Any<int>());
            _logger.Received().Warning("Sink handler is not specified. Check if the handler is configured properly, and restart the connector.");
            Assert.True(batch.All(r=>r.Skip));
            Assert.True(batch.All(r=>r.Status == SinkStatus.Skipped));
        } 
        
        [Fact]
        public async Task Sink_WhenHasAValidHandler()
        {
            var batch = GetBatch();
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig() { Parallelism = 1 });

            await _sinkProcessor.Sink(batch, "connector", 0);
            _sinkHandlerProvider.Received().GetSinkHandler(Arg.Any<string>());
            await _sinkHandler.Received().Put(Arg.Any<ConnectRecordBatch>(), Arg.Any<string>(), Arg.Any<int>());
            _logger.DidNotReceive().Warning("Sink handler is not specified. Check if the handler is configured properly, and restart the connector.");
        } 
        
        private static ConnectRecordBatch GetBatch(int length = 1, params (string topic, int partition)[] topicPartitions)
        {
            var batch = new ConnectRecordBatch("connector");

            for (var i = 0; i < length; i++)
            {
                var (topic, partition) = topicPartitions != null && topicPartitions.Length > i ? topicPartitions[i] : ("topic", 0);
                batch.Add(new SinkRecord(new ConsumeResult<byte[], byte[]>
                    {Topic = topic, Partition = partition, Message = new Message<byte[], byte[]>() {Headers = new Headers()}}));
            }

            return batch;
        }
    }
}