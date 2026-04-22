using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Connectors;

public class ConnectorClientTests
{
    private readonly IKafkaClientBuilder _kafkaClientBuilder = Substitute.For<IKafkaClientBuilder>();
    private readonly IConfigurationProvider _configurationProvider = Substitute.For<IConfigurationProvider>();
    private readonly IExecutionContext _executionContext = Substitute.For<IExecutionContext>();
    private readonly ILogger<ConnectorClient> _logger = Substitute.For<ILogger<ConnectorClient>>();

    [Fact]
    public void TryBuildSubscriber_WhenTopicsMissing_ReturnsFalse()
    {
        _configurationProvider.GetTopics("orders").Returns(new List<string>());
        var subject = CreateSubject();

        var actual = subject.TryBuildSubscriber("orders", 1);

        Assert.False(actual);
    }

    [Fact]
    public void TryBuildSubscriber_WhenConsumerIsCreated_SubscribesAndReturnsTrue()
    {
        var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
        _configurationProvider.GetTopics("orders").Returns(new List<string> { "topic-a" });
        _kafkaClientBuilder.GetConsumer("orders", 1).Returns(consumer);

        var subject = CreateSubject();

        var actual = subject.TryBuildSubscriber("orders", 1);

        Assert.True(actual);
        consumer.Received(1).Subscribe(Arg.Is<IEnumerable<string>>(s => s.Single() == "topic-a"));
    }

    [Fact]
    public void TryBuildPublisher_WhenProducerMissing_ReturnsFalse()
    {
        _kafkaClientBuilder.GetProducer("orders").Returns((IProducer<byte[], byte[]>)null!);
        var subject = CreateSubject();

        var actual = subject.TryBuildPublisher("orders");

        Assert.False(actual);
    }

    [Fact]
    public void Commit_WhenAutoCommitDisabled_CommitsMaxOffsets()
    {
        var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
        _configurationProvider.GetTopics("orders").Returns(new List<string> { "topic-a" });
        _kafkaClientBuilder.GetConsumer("orders", 1).Returns(consumer);
        _configurationProvider.GetAutoCommitConfig().Returns((false, false));

        var subject = CreateSubject();
        Assert.True(subject.TryBuildSubscriber("orders", 1));

        subject.Commit(
        [
            ("topic-a", 0, 10L),
            ("topic-a", 0, 12L),
            ("topic-a", 1, 4L)
        ]);

        consumer.Received(1).Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>());
    }

    [Fact]
    public void Commit_WhenAutoOffsetStoreEnabledFalse_StoresOffsets()
    {
        var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
        _configurationProvider.GetTopics("orders").Returns(new List<string> { "topic-a" });
        _kafkaClientBuilder.GetConsumer("orders", 1).Returns(consumer);
        _configurationProvider.GetAutoCommitConfig().Returns((true, false));

        var subject = CreateSubject();
        Assert.True(subject.TryBuildSubscriber("orders", 1));

        subject.Commit([("topic-a", 0, 10L)]);

        consumer.Received(1).StoreOffset(Arg.Any<TopicPartitionOffset>());
    }

    [Fact]
    public async Task NotifyEndOfPartition_WhenEnabled_ProducesEofMessages()
    {
        var producer = Substitute.For<IProducer<byte[], byte[]>>();
        producer
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>())
            .Returns(Task.FromResult(new DeliveryResult<byte[], byte[]>
            {
                Topic = "eof-topic",
                Partition = new Partition(0),
                Offset = new Offset(15)
            }));

        _kafkaClientBuilder.GetProducer("orders").Returns(producer);
        _configurationProvider.GetEofSignalConfig("orders").Returns(new EofConfig
        {
            Enabled = true,
            Topic = "eof-topic"
        });

        var subject = CreateSubject();
        Assert.True(subject.TryBuildPublisher("orders"));

        await subject.NotifyEndOfPartition(
            "orders",
            1,
            [("topic-a", 0, 11L)],
            [("topic-a", 0, 10L)]);

        await producer.Received(1).ProduceAsync("eof-topic", Arg.Any<Message<byte[], byte[]>>());
    }

    [Fact]
    public async Task SendToDeadLetter_OnlyProducesForFailedRecords()
    {
        var producer = Substitute.For<IProducer<byte[], byte[]>>();
        producer
            .ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>())
            .Returns(Task.FromResult(new DeliveryResult<byte[], byte[]>
            {
                Topic = "dead-letter-topic",
                Partition = new Partition(1),
                Offset = new Offset(33)
            }));

        _kafkaClientBuilder.GetProducer("orders").Returns(producer);
        _configurationProvider.GetErrorsConfig("orders").Returns(new ErrorsConfig { Topic = "dead-letter-topic" });

        var subject = CreateSubject();
        Assert.True(subject.TryBuildPublisher("orders"));

        var failed = new ConnectRecord("topic-a", 0, 1)
        {
            Status = Status.Failed,
            Serialized = new ConnectMessage<byte[]>
            {
                Key = [1],
                Value = [2],
                Headers = new Dictionary<string, byte[]>()
            }
        };

        var ok = new ConnectRecord("topic-a", 0, 2)
        {
            Status = Status.Processed,
            Serialized = new ConnectMessage<byte[]>
            {
                Key = [3],
                Value = [4],
                Headers = new Dictionary<string, byte[]>()
            }
        };

        await subject.SendToDeadLetter([failed, ok], "orders", 1, "upsert");

        await producer.Received(1).ProduceAsync("dead-letter-topic", Arg.Any<Message<byte[], byte[]>>());
    }

    [Fact]
    public void Close_DisposesConsumerAndProducer()
    {
        var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
        var producer = Substitute.For<IProducer<byte[], byte[]>>();

        _configurationProvider.GetTopics("orders").Returns(new List<string> { "topic-a" });
        _kafkaClientBuilder.GetConsumer("orders", 1).Returns(consumer);
        _kafkaClientBuilder.GetProducer("orders").Returns(producer);

        var subject = CreateSubject();
        Assert.True(subject.TryBuildSubscriber("orders", 1));
        Assert.True(subject.TryBuildPublisher("orders"));

        subject.Close();

        consumer.Received(1).Close();
        consumer.Received(1).Dispose();
        producer.Received(1).Dispose();
    }

    private ConnectorClient CreateSubject() => new(_kafkaClientBuilder, _configurationProvider, _executionContext, _logger);
}
