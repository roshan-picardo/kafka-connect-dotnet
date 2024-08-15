using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Builders
{
    public class KafkaClientBuilderTests
    {
        private readonly IConfigurationProvider _configurationProvider;
        private readonly KafkaClientBuilder _kafkaClientBuilder;

        public KafkaClientBuilderTests()
        {
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _kafkaClientBuilder = new KafkaClientBuilder(Substitute.For<ILogger<KafkaClientBuilder>>(),
                _configurationProvider, Substitute.For<IKafkaClientEventHandler>());
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