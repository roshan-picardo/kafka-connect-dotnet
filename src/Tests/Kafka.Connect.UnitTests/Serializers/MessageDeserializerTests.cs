using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Providers;
using Kafka.Connect.Serializers;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Serializers
{
    public class MessageConverterTests
    {
        private readonly IProcessorServiceProvider _processorServiceProvider;
        private readonly IMessageConverter _messageConverter;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IDeserializer _deserializer;

        public MessageConverterTests()
        {
            _processorServiceProvider = Substitute.For<IProcessorServiceProvider>();
            _deserializer = Substitute.For<IDeserializer>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _messageConverter = new MessageConverter(Substitute.For<ILogger<MessageConverter>>(),
                _processorServiceProvider, _configurationProvider);
        }

        [Fact]
        public async Task Deserialize()
        {
            var keyToken = new JObject {{"json", "this is a key token sample!"}};
            var valueToken = new JObject {{"json", "this is a value token sample!"}};
            _configurationProvider.GetDeserializers(Arg.Any<string>(), Arg.Any<string>()).Returns(("key", "value"));
            _processorServiceProvider.GetDeserializer(Arg.Any<string>()).Returns(_deserializer);
            _processorServiceProvider.GetDeserializer(Arg.Any<string>()).Returns(_deserializer);
            _deserializer.Deserialize(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<string>(), Arg.Any<Dictionary<string, byte[]>>(), Arg.Any<bool>())
                .Returns(_ => keyToken, _ => valueToken);

            var (expectedKey, expectedValue) = await _messageConverter.Deserialize("", new Message<byte[], byte[]>(), "");

            Assert.Equal(keyToken, expectedKey);
            Assert.Equal(valueToken, expectedValue);
            await _deserializer.Received(2)
                .Deserialize(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<string>(), Arg.Any<IDictionary<string, byte[]>>(), Arg.Any<bool>());
        }
    }
}