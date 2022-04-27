using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Builders
{
    public class KafkaClientBuilderTests
    {
        private readonly IExecutionContext _executionContext;
        private readonly ILogger<KafkaClientBuilder> _logger;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly KafkaClientBuilder _kafkaClientBuilder;

        public KafkaClientBuilderTests()
        {
            _logger = Substitute.For<ILogger<KafkaClientBuilder>>();
            _executionContext = Substitute.For<IExecutionContext>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _kafkaClientBuilder = new KafkaClientBuilder(_logger, _executionContext, _configurationProvider);
        }

        [Fact]
        public void GetConsumer_CreatesConsumer()
        {
            _configurationProvider.GetConsumerConfig(Arg.Any<string>()).Returns(new ConsumerConfig()
                {BootstrapServers = "localhost:9092", CancellationDelayMaxMs = 1001, GroupId = "test-group"});
            var actual = _kafkaClientBuilder.GetConsumer("");

        }

    }
}