using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Config;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Serializers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Handlers
{
    public class SinkProcessorTests
    {
        private readonly IMessageConverter _messageConverter;
        private readonly IMessageHandler _messageHandler;
        private readonly ISinkHandlerProvider _sinkHandlerProvider;
        private readonly SinkProcessor _sinkProcessor;

        public SinkProcessorTests()
        {
            var logger = Substitute.For<ILogger<SinkProcessor>>();
            _messageConverter = Substitute.For<IMessageConverter>();
            _messageHandler = Substitute.For<IMessageHandler>();
            _sinkHandlerProvider = Substitute.For<ISinkHandlerProvider>();

            _sinkProcessor = new SinkProcessor(logger, _messageConverter, _messageHandler, _sinkHandlerProvider);
        }

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
    }
}