using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Connectors
{
    public class ConnectDeadLetterTests
    {
        private readonly ILogger<ConnectDeadLetter> _logger;
        private readonly IKafkaClientBuilder _kafkaClientBuilder;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly ConnectDeadLetter _connectDeadLetter;
        private readonly IProducer<byte[], byte[]> _producer;

        public ConnectDeadLetterTests()
        {
            _logger = Substitute.For<ILogger<ConnectDeadLetter>>();
            _kafkaClientBuilder = Substitute.For<IKafkaClientBuilder>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _producer = Substitute.For<IProducer<byte[], byte[]>>();
            _connectDeadLetter = new ConnectDeadLetter(_logger, _kafkaClientBuilder, _configurationProvider);
        }


        [Fact]
        public async Task Send_Tests()
        {
            var errorsConfig = new ErrorsConfig { Topic = "dead-letter-topic" };
            _configurationProvider.GetErrorsConfig(Arg.Any<string>()).Returns(errorsConfig);
            _kafkaClientBuilder.GetProducer(Arg.Any<string>()).Returns(_producer);
            var consumed = new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>(),
                Topic = "read-topic",
                Partition = 2,
                Offset = 100
            };
            var exception = new Exception("Current data exception");
            var records = new List<global::Kafka.Connect.Models.SinkRecord>() { new(consumed) };
            var deliveryResult = new DeliveryResult<byte[], byte[]>()
                { Topic = "dead-letter-topic", Partition = 2, Offset = 200 };
            _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>()).Returns(deliveryResult);

            await _connectDeadLetter.Send(records, exception, "connector");

            _configurationProvider.Received().GetErrorsConfig("connector");
            _kafkaClientBuilder.Received().GetProducer("connector");
            await _producer.Received().ProduceAsync("dead-letter-topic", records.First().GetDeadLetterMessage(exception));
            _logger.Received().Info("Error message delivered.", Arg.Any<object>());
        }
    }
}