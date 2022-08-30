using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Builders
{
    public class KafkaClientBuilderTests
    {
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IKafkaClientEventHandler _kafkaClientEventHandler;
        private readonly KafkaClientBuilder _kafkaClientBuilder;

        public KafkaClientBuilderTests()
        {
            Substitute.For<ILogger<KafkaClientBuilder>>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _kafkaClientEventHandler = Substitute.For<IKafkaClientEventHandler>();

            _kafkaClientBuilder = new KafkaClientBuilder(_configurationProvider, _kafkaClientEventHandler);
        }

        [Fact]
        public void GetConsumer_ReturnsConsumer()
        {
            _configurationProvider.GetConsumerConfig(Arg.Any<string>())
                .Returns(new ConsumerConfig {BootstrapServers = "localhost:9092", GroupId = "test-group"});
            
            Assert.NotNull(_kafkaClientBuilder.GetConsumer("connector", 1));
        }
        
        [Fact]
        public void GetProducer_ReturnsProducer()
        {
            _configurationProvider.GetProducerConfig(Arg.Any<string>())
                .Returns(new ProducerConfig{BootstrapServers = "localhost:9092"});
            
            Assert.NotNull(_kafkaClientBuilder.GetProducer("connector"));
        }
        
        [Fact]
        public void GetAdminClient_ReturnsAdminClient()
        {
            _configurationProvider.GetConsumerConfig(Arg.Any<string>())
                .Returns(new ConsumerConfig{BootstrapServers = "localhost:9092"});
            
            Assert.NotNull(_kafkaClientBuilder.GetAdminClient("connector"));
        }

    }
}