using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Providers;
using Kafka.Connect.Serializers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Serializers
{
    public class MessageDeserializerTests
    {
        private readonly IProcessorServiceProvider _processorServiceProvider;
        private readonly IMessageConverter _messageConverter;
        private readonly IDeserializer _deserializer;

        public MessageDeserializerTests()
        {
            _processorServiceProvider = Substitute.For<IProcessorServiceProvider>();
            _deserializer = Substitute.For<IDeserializer>();
            _messageConverter = new MessageConverter(Substitute.For<ILogger<MessageConverter>>(),
                _processorServiceProvider);
        }

        [Fact]
        public async Task Deserialize()
        {
            var keyToken = new JObject {{"json", "this is a key token sample!"}};
            var valueToken = new JObject {{"json", "this is a value token sample!"}};
            //new JObject {{"json", "this is a test sample!"}}
            _processorServiceProvider.GetDeserializer(Arg.Any<string>()).Returns(_deserializer);
            _processorServiceProvider.GetDeserializer(Arg.Any<string>()).Returns(_deserializer);
            _deserializer.Deserialize(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<SerializationContext>(), Arg.Any<bool>())
                .Returns(d => keyToken, d => valueToken);

            var (expectedKey, expectedValue) = await _messageConverter.Deserialize(new ConverterConfig(),
                new ConsumeResult<byte[], byte[]> {Message = new Message<byte[], byte[]>()});

            Assert.Equal(keyToken, expectedKey);
            Assert.Equal(valueToken, expectedValue);
            await _deserializer.Received(2)
                .Deserialize(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<SerializationContext>(), Arg.Any<bool>());
        }
    }
}