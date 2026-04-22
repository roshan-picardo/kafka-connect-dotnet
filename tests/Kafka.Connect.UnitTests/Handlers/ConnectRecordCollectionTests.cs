using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Handlers;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Handlers;

public class ConnectRecordCollectionTests
{
    private readonly ILogger<ConnectRecordCollection> _logger = Substitute.For<ILogger<ConnectRecordCollection>>();
    private readonly IMessageHandler _messageHandler = Substitute.For<IMessageHandler>();
    private readonly global::Kafka.Connect.Providers.IConfigurationProvider _configurationProvider = Substitute.For<global::Kafka.Connect.Providers.IConfigurationProvider>();
    private readonly IExecutionContext _executionContext = Substitute.For<IExecutionContext>();
    private readonly IConfigurationChangeHandler _configurationChangeHandler = Substitute.For<IConfigurationChangeHandler>();
    private readonly IConnectorClient _connectorClient = Substitute.For<IConnectorClient>();
    private readonly ILogRecord _logRecord = Substitute.For<ILogRecord>();
    private readonly IPluginHandler _pluginHandler = Substitute.For<IPluginHandler>();

    private ConnectRecordCollection CreateCollection(IEnumerable<IPluginHandler> pluginHandlers = null)
    {
        _configurationProvider.GetParallelRetryOptions(Arg.Any<string>())
            .Returns(new ParallelRetryOptions { Attempts = 1, DegreeOfParallelism = 1 });
        _configurationProvider.GetPluginConfig(Arg.Any<string>())
            .Returns(new PluginConfig { Name = "plugin", Handler = "handler" });
        _configurationProvider.GetBatchConfig(Arg.Any<string>())
            .Returns(new BatchConfig { Size = 25, Parallelism = 1, Interval = 1000, Poll = 100 });
        _configurationProvider.GetTopic(TopicType.Command).Returns("commands");
        _configurationProvider.IsErrorTolerated(Arg.Any<string>()).Returns(true);
        _pluginHandler.Is(Arg.Any<string>(), "plugin", "handler").Returns(true);

        return new ConnectRecordCollection(
            _logger,
            _messageHandler,
            _configurationProvider,
            pluginHandlers ?? new[] { _pluginHandler },
            _executionContext,
            _configurationChangeHandler,
            _connectorClient,
            new[] { _logRecord });
    }

    [Fact]
    public async Task TrySubscribeAndTryPublisher_DelegateToConnectorClient()
    {
        var collection = CreateCollection(pluginHandlers: []);
        await collection.Setup(ConnectorType.Worker, "orders", 2);
        _connectorClient.TryBuildSubscriber("orders", 2).Returns(true);
        _connectorClient.TryBuildPublisher("orders").Returns(true);

        Assert.True(collection.TrySubscribe());
        Assert.True(collection.TryPublisher());
    }

    [Fact]
    public async Task Setup_WhenSinkConnector_StartsPluginHandler()
    {
        var collection = CreateCollection();

        await collection.Setup(ConnectorType.Sink, "orders", 3);

        await _pluginHandler.Received(1).Startup("orders");
    }

    [Fact]
    public async Task Purge_WhenSourceConnector_CallsPluginHandlerPurge()
    {
        var collection = CreateCollection();

        await collection.Purge(ConnectorType.Source, "orders", 1);

        await _pluginHandler.Received(1).Purge("orders");
    }

    [Fact]
    public async Task Consume_TracksRegularRecordsAndCommitUsesEofOffsets()
    {
        var collection = CreateCollection(pluginHandlers: []);
        await collection.Setup(ConnectorType.Worker, "orders", 7);

        _connectorClient.Consume("orders", 7, Arg.Any<CancellationToken>()).Returns(new List<SinkRecord>
        {
            CreateSinkRecord("topic-a", 0, 10, isEof: true),
            CreateSinkRecord("topic-a", 0, 5)
        });

        await collection.Consume(CancellationToken.None);

        Assert.Equal(1, collection.Count());

        var commands = new List<CommandRecord>
        {
            new() { Connector = "orders", Name = "sync", Topic = "commands", Partition = 0, Command = new JsonObject() }
        };

        collection.Commit(commands);

        _executionContext.Received(1).SetPartitionEof("orders", 7, "commands", 0, false);
        _connectorClient.Received(1).Commit(Arg.Is<IList<(string Topic, int Partition, long Offset)>>(offsets =>
            offsets.Count == 1 &&
            offsets[0].Topic == "topic-a" &&
            offsets[0].Partition == 0 &&
            offsets[0].Offset == 9));
    }

    [Fact]
    public async Task ConfigureAndProcess_SourceBatch_SerializesAndProducesRecords()
    {
        var collection = CreateCollection(pluginHandlers: []);
        await collection.Setup(ConnectorType.Worker, "orders", 1);

        var configuredRecord = new ConnectRecord("config-topic", 0, 0)
        {
            Status = Status.Selected,
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = JsonValue.Create("billing"),
                Value = JsonNode.Parse("{\"workers\":[\"worker-a\"]}")
            }
        };
        _configurationChangeHandler.Configure("billing", Arg.Any<JsonObject>()).Returns(configuredRecord);

        var processedMessage = new ConnectMessage<JsonNode>
        {
            Key = JsonValue.Create("billing"),
            Value = JsonNode.Parse("{\"workers\":[\"worker-a\"],\"ok\":true}")
        };
        _messageHandler.Process("orders", "config-topic", Arg.Any<ConnectMessage<JsonNode>>())
            .Returns((false, processedMessage));
        _messageHandler.Serialize("orders", "config-topic", processedMessage)
            .Returns(new ConnectMessage<byte[]> { Key = [1], Value = [2] });

        collection.Configure("batch-1", ("billing", new JsonObject()));

        Assert.Equal(1, collection.Count("batch-1"));

        await collection.Process("batch-1");
        await collection.Produce("batch-1");

        await _messageHandler.Received(1).Process("orders", "config-topic", Arg.Any<ConnectMessage<JsonNode>>());
        await _messageHandler.Received(1).Serialize("orders", "config-topic", processedMessage);
        await _connectorClient.Received(1).Produce("orders", Arg.Is<List<IConnectRecord>>(records => records.Count == 1));
    }

    [Fact]
    public async Task Sink_WhenPluginHandlerMissing_ThrowsConnectDataException()
    {
        var collection = CreateCollection(pluginHandlers: []);
        await collection.Setup(ConnectorType.Worker, "orders", 1);

        _connectorClient.Consume("orders", 1, Arg.Any<CancellationToken>()).Returns(new List<SinkRecord>
        {
            CreateSinkRecord("topic-a", 0, 1)
        });

        await collection.Consume(CancellationToken.None);

        await Assert.ThrowsAsync<ConnectDataException>(() => collection.Sink());
    }

    [Fact]
    public async Task Refresh_UsesLatestRecordsAndOnlyRefreshesRelevantWorkerChanges()
    {
        var collection = CreateCollection(pluginHandlers: []);
        await collection.Setup(ConnectorType.Worker, "orders", 1);

        _connectorClient.Consume("orders", 1, Arg.Any<CancellationToken>()).Returns(new List<SinkRecord>
        {
            CreateSinkRecord("topic-a", 0, 1),
            CreateSinkRecord("topic-a", 0, 2),
            CreateSinkRecord("topic-a", 0, 3)
        });

        var deserialized = new Queue<ConnectMessage<JsonNode>>(new[]
        {
            new ConnectMessage<JsonNode> { Key = JsonValue.Create("delete-me"), Value = JsonNode.Parse("{}") },
            new ConnectMessage<JsonNode> { Key = JsonValue.Create("assigned"), Value = JsonNode.Parse("{\"workers\":[\"worker-a\"]}") },
            new ConnectMessage<JsonNode> { Key = JsonValue.Create("other-worker"), Value = JsonNode.Parse("{\"workers\":[\"worker-b\"]}") }
        });
        _messageHandler.Deserialize("orders", "topic-a", Arg.Any<ConnectMessage<byte[]>>())
            .Returns(_ => deserialized.Dequeue());
        _messageHandler.Process("orders", "topic-a", Arg.Any<ConnectMessage<JsonNode>>())
            .Returns(call => (false, call.ArgAt<ConnectMessage<JsonNode>>(2)));

        await collection.Consume(CancellationToken.None);
        await collection.Process();
        await collection.Refresh("worker-a");

        await _executionContext.Received(1).Refresh("delete-me", true);
        await _executionContext.Received(1).Refresh("assigned", false);
        await _executionContext.DidNotReceive().Refresh("other-worker", Arg.Any<bool>());
    }

    [Fact]
    public async Task Store_DelegatesToConfigurationChangeHandler()
    {
        var collection = CreateCollection(pluginHandlers: []);
        await collection.Setup(ConnectorType.Worker, "orders", 1);

        _connectorClient.Consume("orders", 1, Arg.Any<CancellationToken>()).Returns(new List<SinkRecord>
        {
            CreateSinkRecord("topic-a", 0, 1)
        });

        await collection.Consume(CancellationToken.None);
        await collection.Store(refresh: true);
        await collection.Store("worker-a");

        await _configurationChangeHandler.Received(1).Store(Arg.Any<IEnumerable<ConnectRecord>>(), true);
        await _configurationChangeHandler.Received(1).Store(Arg.Any<IEnumerable<ConnectRecord>>(), "worker-a");
    }

    [Fact]
    public async Task Commit_UsesCommitReadyOffsetsUntilAbortedRecord()
    {
        var collection = CreateCollection(pluginHandlers: []);
        await collection.Setup(ConnectorType.Worker, "orders", 3);

        _connectorClient.Consume("orders", 3, Arg.Any<CancellationToken>()).Returns(new List<SinkRecord>
        {
            CreateSinkRecord("topic-a", 0, 1),
            CreateSinkRecord("topic-a", 0, 2),
            CreateSinkRecord("topic-a", 0, 3)
        });

        await collection.Consume(CancellationToken.None);
        collection.UpdateTo(Status.Aborted, "topic-a", 0, 2);

        collection.Commit();

        _connectorClient.Received(1).Commit(Arg.Is<IList<(string Topic, int Partition, long Offset)>>(offsets =>
            offsets.Count == 1 &&
            offsets[0].Topic == "topic-a" &&
            offsets[0].Partition == 0 &&
            offsets[0].Offset == 1));
    }

    [Fact]
    public async Task DeadLetter_ForwardsCurrentBatchToConnectorClient()
    {
        var collection = CreateCollection(pluginHandlers: []);
        await collection.Setup(ConnectorType.Worker, "orders", 4);

        _connectorClient.Consume("orders", 4, Arg.Any<CancellationToken>()).Returns(new List<SinkRecord>
        {
            CreateSinkRecord("topic-a", 0, 11)
        });

        await collection.Consume(CancellationToken.None);
        await collection.DeadLetter();

        await _connectorClient.Received(1).SendToDeadLetter(Arg.Is<IEnumerable<ConnectRecord>>(records => records.Count() == 1), "orders", 4, null);
    }

    [Fact]
    public async Task NotifyEndOfPartition_SendsEofAndCommitReadyOffsets()
    {
        var collection = CreateCollection(pluginHandlers: []);
        await collection.Setup(ConnectorType.Worker, "orders", 5);

        _connectorClient.Consume("orders", 5, Arg.Any<CancellationToken>()).Returns(new List<SinkRecord>
        {
            CreateSinkRecord("topic-a", 0, 20, isEof: true),
            CreateSinkRecord("topic-a", 0, 19)
        });

        await collection.Consume(CancellationToken.None);
        await collection.NotifyEndOfPartition();

        await _connectorClient.Received(1).NotifyEndOfPartition(
            "orders",
            5,
            Arg.Is<IList<(string Topic, int Partition, long Offset)>>(offsets =>
                offsets.Count == 1 && offsets[0].Topic == "topic-a" && offsets[0].Partition == 0 && offsets[0].Offset == 20),
            Arg.Is<IList<(string Topic, int Partition, long Offset)>>(offsets =>
                offsets.Count == 1 && offsets[0].Topic == "topic-a" && offsets[0].Partition == 0 && offsets[0].Offset == 19));
    }

    [Fact]
    public async Task UpdateCommand_WhenBatchExists_UsesPluginNextCommandAndPublishesSerializedMessage()
    {
        var collection = CreateCollection();
        await collection.Setup(ConnectorType.Sink, "orders", 6);

        var command = new CommandRecord
        {
            Connector = "orders",
            Name = "sync",
            Topic = "commands",
            Partition = 2,
            Command = new JsonObject { ["seed"] = true }
        };

        var configuredRecord = new ConnectRecord("topic-a", 0, 0)
        {
            Status = Status.Selected,
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = JsonValue.Create("billing"),
                Value = JsonNode.Parse("{\"workers\":[\"worker-a\"]}")
            }
        };
        _configurationChangeHandler.Configure("billing", Arg.Any<JsonObject>()).Returns(configuredRecord);

        var nextCommand = new JsonObject { ["refresh"] = true };
        _pluginHandler.NextCommand(command, Arg.Any<List<ConnectRecord>>()).Returns(nextCommand);
        _messageHandler.Serialize("orders", "commands", Arg.Any<ConnectMessage<JsonNode>>())
            .Returns(new ConnectMessage<byte[]> { Key = [7], Value = [8] });

        collection.Configure(command.Id.ToString(), ("billing", new JsonObject()));

        var actual = await collection.UpdateCommand(command);

        Assert.Same(nextCommand, actual);
        await _connectorClient.Received(1).Produce(
            Arg.Is<TopicPartition>(tp => tp.Topic == "commands" && tp.Partition == 2),
            Arg.Is<Message<byte[], byte[]>>(message => message.Key.SequenceEqual(new byte[] { 7 }) && message.Value.SequenceEqual(new byte[] { 8 })));
    }

    [Fact]
    public async Task GetCommands_ReturnsPluginCommandsWithExistingOffsetsAndCommandPayload()
    {
        var collection = CreateCollection();
        await collection.Setup(ConnectorType.Sink, "orders", 7);

        var commandDefinition = new TestCommand { Topic = "topic-a", Version = 3 };
        var expected = new CommandRecord
        {
            Connector = "orders",
            Name = "sync",
            Topic = "commands",
            Command = commandDefinition.ToJson()
        };

        _pluginHandler.Commands("orders").Returns(new Dictionary<string, Command>
        {
            ["sync"] = commandDefinition
        });
        var assignedPartition = (expected.Id.GetHashCode() & 0x7FFFFFFF) % 50;
        _executionContext.GetAssignedPartitions("orders", 7).Returns(new Dictionary<string, List<int>>
        {
            ["commands"] = [assignedPartition]
        });

        _connectorClient.Consume("orders", 7, Arg.Any<CancellationToken>()).Returns(new List<SinkRecord>
        {
            CreateSinkRecord("source-topic", 4, 99)
        });
        _messageHandler.Deserialize("orders", "source-topic", Arg.Any<ConnectMessage<byte[]>>()).Returns(new ConnectMessage<JsonNode>
        {
            Key = JsonValue.Create(expected.Id.ToString()),
            Value = JsonSerializer.SerializeToNode(new CommandRecord
            {
                Connector = "orders",
                Name = "sync",
                Topic = "source-topic",
                Partition = 4,
                Offset = 99,
                Command = commandDefinition.ToJson()
            })
        });

        await collection.Consume(CancellationToken.None);

        var commands = await collection.GetCommands();

        var actual = Assert.Single(commands);
        Assert.Equal(4, actual.Partition);
        Assert.Equal(99, actual.Offset);
        Assert.Equal("source-topic", actual.Topic);
        Assert.Equal(commandDefinition.Version, actual.Command?["Version"]?.GetValue<int>());
    }

    [Fact]
    public async Task Source_AddsPluginRecordsToCommandBatch()
    {
        var collection = CreateCollection();
        await collection.Setup(ConnectorType.Source, "orders", 8);

        var command = new CommandRecord
        {
            Connector = "orders",
            Name = "sync",
            Topic = "commands",
            Partition = 1,
            Command = new JsonObject()
        };
        _pluginHandler.Get("orders", 8, command).Returns(new List<ConnectRecord>
        {
            new ConnectRecord("topic-a", 0, 1)
            {
                Status = Status.Selected,
                Deserialized = new ConnectMessage<JsonNode> { Key = JsonValue.Create("k1"), Value = JsonValue.Create("v1") }
            },
            new ConnectRecord("topic-a", 0, 2)
            {
                Status = Status.Selected,
                Deserialized = new ConnectMessage<JsonNode> { Key = JsonValue.Create("k2"), Value = JsonValue.Create("v2") }
            }
        });

        await collection.Source(command);

        Assert.Equal(2, collection.Count(command.Id.ToString()));
    }

    [Fact]
    public async Task Sink_WithPluginHandler_PutsBatch()
    {
        var collection = CreateCollection();
        await collection.Setup(ConnectorType.Sink, "orders", 9);

        _connectorClient.Consume("orders", 9, Arg.Any<CancellationToken>()).Returns(new List<SinkRecord>
        {
            CreateSinkRecord("topic-a", 0, 1)
        });

        await collection.Consume(CancellationToken.None);
        await collection.Sink();

        await _pluginHandler.Received(1).Put(Arg.Is<IList<ConnectRecord>>(records => records.Count == 1), "orders", 9);
    }

    [Fact]
    public async Task Record_LogsBatchAndCleanupClearsCollections()
    {
        var collection = CreateCollection(pluginHandlers: []);
        await collection.Setup(ConnectorType.Worker, "orders", 10);
        _configurationProvider.GetLogEnhancer("orders").Returns(_logRecord.GetType().FullName);

        _connectorClient.Consume("orders", 10, Arg.Any<CancellationToken>()).Returns(new List<SinkRecord>
        {
            CreateSinkRecord("topic-a", 0, 1)
        });

        await collection.Consume(CancellationToken.None);
        collection.UpdateTo(Status.Processing, "topic-a", 0, 1);
        collection.StartTiming();
        collection.EndTiming();
        collection.Record();

        _logger.Received(1).Record(Arg.Any<object>(), Arg.Any<Exception>());

        collection.Cleanup();

        Assert.Equal(0, collection.Count());
        _connectorClient.Received(1).Close();
    }

    [Fact]
    public async Task ClearAndClearAll_RemoveSinkAndSourceBatches()
    {
        var collection = CreateCollection(pluginHandlers: []);
        await collection.Setup(ConnectorType.Worker, "orders", 11);

        _connectorClient.Consume("orders", 11, Arg.Any<CancellationToken>()).Returns(new List<SinkRecord>
        {
            CreateSinkRecord("topic-a", 0, 1)
        });

        var configuredRecord = new ConnectRecord("config-topic", 0, 0)
        {
            Status = Status.Selected,
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = JsonValue.Create("billing"),
                Value = JsonNode.Parse("{\"workers\":[\"worker-a\"]}")
            }
        };
        _configurationChangeHandler.Configure("billing", Arg.Any<JsonObject>()).Returns(configuredRecord);

        await collection.Consume(CancellationToken.None);
        collection.Configure("batch-1", ("billing", new JsonObject()));
        collection.Clear();

        Assert.Equal(0, collection.Count());
        Assert.Equal(1, collection.Count("batch-1"));

        collection.ClearAll();

        Assert.Equal(0, collection.Count("batch-1"));
    }

    private static SinkRecord CreateSinkRecord(string topic, int partition, long offset, bool isEof = false)
    {
        return new SinkRecord(new ConsumeResult<byte[], byte[]>
        {
            Topic = topic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            IsPartitionEOF = isEof,
            Message = new Message<byte[], byte[]>
            {
                Key = [1],
                Value = [2],
                Timestamp = new Timestamp(0, TimestampType.CreateTime),
                Headers = new Headers()
            }
        });
    }

    private sealed class TestCommand : Command
    {
        public override JsonNode ToJson() => new JsonObject
        {
            ["Topic"] = Topic,
            ["Version"] = Version
        };
    }
}
