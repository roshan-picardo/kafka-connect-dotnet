using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Builders;

public class KafkaClientBuilderTests
{
    private readonly ILogger<KafkaClientBuilder> _logger;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IKafkaClientEventHandler _eventHandler;
    private readonly KafkaClientBuilder _kafkaClientBuilder;

    public KafkaClientBuilderTests()
    {
        _logger = Substitute.For<ILogger<KafkaClientBuilder>>();
        _configurationProvider = Substitute.For<IConfigurationProvider>();
        _eventHandler = Substitute.For<IKafkaClientEventHandler>();
        _kafkaClientBuilder = new KafkaClientBuilder(_logger, _configurationProvider, _eventHandler);
    }

    // ── GetConsumer ──────────────────────────────────────────────────────────

    [Fact]
    public void GetConsumer_ReturnsConsumer()
    {
        _configurationProvider.GetConsumerConfig(Arg.Any<string>())
            .Returns(new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "test-group" });

        Assert.NotNull(_kafkaClientBuilder.GetConsumer("connector", 1));
    }

    [Fact]
    public void GetConsumer_PassesConnectorToConfigProvider()
    {
        _configurationProvider.GetConsumerConfig(Arg.Any<string>())
            .Returns(new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "test-group" });

        _kafkaClientBuilder.GetConsumer("my-connector", 2);

        _configurationProvider.Received(1).GetConsumerConfig("my-connector");
    }

    [Fact]
    public void GetConsumer_LogsTrackMessage()
    {
        _configurationProvider.GetConsumerConfig(Arg.Any<string>())
            .Returns(new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "test-group" });

        _kafkaClientBuilder.GetConsumer("connector", 1);

        _logger.Received(1).Track("Creating message consumer.");
    }

    // ── GetProducer(string connector) ────────────────────────────────────────

    [Fact]
    public void GetProducer_ByConnector_ReturnsProducer()
    {
        _configurationProvider.GetProducerConfig(Arg.Any<string>())
            .Returns(new ProducerConfig { BootstrapServers = "localhost:9092" });

        Assert.NotNull(_kafkaClientBuilder.GetProducer("connector"));
    }

    [Fact]
    public void GetProducer_ByConnector_PassesConnectorToConfigProvider()
    {
        _configurationProvider.GetProducerConfig(Arg.Any<string>())
            .Returns(new ProducerConfig { BootstrapServers = "localhost:9092" });

        _kafkaClientBuilder.GetProducer("my-connector");

        _configurationProvider.Received(1).GetProducerConfig("my-connector");
    }

    [Fact]
    public void GetProducer_ByConnector_LogsTrackMessage()
    {
        _configurationProvider.GetProducerConfig(Arg.Any<string>())
            .Returns(new ProducerConfig { BootstrapServers = "localhost:9092" });

        _kafkaClientBuilder.GetProducer("connector");

        _logger.Received(1).Track("Creating message producer.");
    }

    // ── GetProducer(ProducerConfig) ──────────────────────────────────────────

    [Fact]
    public void GetProducer_ByConfig_ReturnsProducer()
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        Assert.NotNull(_kafkaClientBuilder.GetProducer(config));
    }

    [Fact]
    public void GetProducer_ByConfig_DoesNotCallConfigProvider()
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        _kafkaClientBuilder.GetProducer(config);

        _configurationProvider.DidNotReceive().GetProducerConfig(Arg.Any<string>());
    }

    [Fact]
    public void GetProducer_ByConfig_LogsTrackMessage()
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        _kafkaClientBuilder.GetProducer(config);

        _logger.Received(1).Track("Creating message producer.");
    }

    // ── GetAdminClient ────────────────────────────────────────────────────────

    [Fact]
    public void GetAdminClient_ReturnsAdminClient()
    {
        _configurationProvider.GetConsumerConfig(Arg.Any<string>())
            .Returns(new ConsumerConfig { BootstrapServers = "localhost:9092" });

        Assert.NotNull(_kafkaClientBuilder.GetAdminClient("connector"));
    }

    [Fact]
    public void GetAdminClient_PassesConnectorToConfigProvider()
    {
        _configurationProvider.GetConsumerConfig(Arg.Any<string>())
            .Returns(new ConsumerConfig { BootstrapServers = "localhost:9092" });

        _kafkaClientBuilder.GetAdminClient("my-connector");

        _configurationProvider.Received(1).GetConsumerConfig("my-connector");
    }

    [Fact]
    public void GetAdminClient_WithNoConnector_PassesNullToConfigProvider()
    {
        _configurationProvider.GetConsumerConfig(null)
            .Returns(new ConsumerConfig { BootstrapServers = "localhost:9092" });

        _kafkaClientBuilder.GetAdminClient();

        _configurationProvider.Received(1).GetConsumerConfig(null);
    }

    [Fact]
    public void GetAdminClient_LogsTrackMessage()
    {
        _configurationProvider.GetConsumerConfig(Arg.Any<string>())
            .Returns(new ConsumerConfig { BootstrapServers = "localhost:9092" });

        _kafkaClientBuilder.GetAdminClient("connector");

        _logger.Received(1).Track("Creating Kafka admin client.");
    }
}