using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Models;

public class ModelBehaviorTests
{
    [Fact]
    public void ConfigRecord_Constructor_SetsTopicOffsetsAndMessage()
    {
        var key = JsonValue.Create("billing");
        var value = JsonNode.Parse("{\"enabled\":true}");

        var actual = new ConfigRecord("config-topic", key, value);

        Assert.Equal("config-topic", actual.Topic);
        Assert.Equal(-1, actual.Partition);
        Assert.Equal(-1, actual.Offset);
        Assert.Equal("billing", actual.Deserialized.Key?.GetValue<string>());
        Assert.True(actual.Deserialized.Value?["enabled"]?.GetValue<bool>());
    }

    [Fact]
    public void SourceRecord_Keys_ReturnsNestedKeyDictionary()
    {
        var actual = new SourceRecord
        {
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = JsonNode.Parse("{\"Keys\":{\"id\":42,\"name\":\"alpha\"}}"),
                Value = JsonNode.Parse("{}")
            }
        };

        Assert.Equal(42, Convert.ToInt32(actual.Keys["id"]));
        Assert.Equal("alpha", actual.Keys["name"]?.ToString());
    }

    [Fact]
    public void Clone_ToSourceRecord_PreservesCoreState()
    {
        var exception = new InvalidOperationException("boom");
        var source = new ConnectRecord("topic-a", 2, 9)
        {
            Status = Status.Processed,
            Exception = exception,
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = JsonValue.Create("k"),
                Value = JsonValue.Create("v")
            },
            Serialized = new ConnectMessage<byte[]>
            {
                Key = [1],
                Value = [2]
            }
        };

        var actual = source.Clone<SourceRecord>();

        Assert.Equal(source.Topic, actual.Topic);
        Assert.Equal(source.Partition, actual.Partition);
        Assert.Equal(source.Offset, actual.Offset);
        Assert.Equal(source.Status, actual.Status);
        Assert.Same(source.Deserialized, actual.Deserialized);
        Assert.Same(source.Serialized, actual.Serialized);
        Assert.Same(exception, actual.Exception);
        Assert.Same(source.LogTimestamp, actual.LogTimestamp);
    }

    [Fact]
    public void SinkRecord_Constructor_MapsConsumedMessageState()
    {
        var consumed = new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders",
            Partition = new Partition(3),
            Offset = new Offset(12),
            IsPartitionEOF = true,
            Message = new Message<byte[], byte[]>
            {
                Key = [1, 2],
                Value = [3, 4],
                Timestamp = new Timestamp(12345, TimestampType.CreateTime),
                Headers = new Headers
                {
                    new Header("tenant", [9])
                }
            }
        };

        var actual = new SinkRecord(consumed);

        Assert.Equal("orders", actual.Topic);
        Assert.Equal(3, actual.Partition);
        Assert.Equal(12, actual.Offset);
        Assert.True(actual.IsPartitionEof);
        Assert.Equal(Status.Consumed, actual.Status);
        Assert.Equal(new byte[] { 1, 2 }, actual.Serialized.Key);
        Assert.Equal(new byte[] { 3, 4 }, actual.Serialized.Value);
        Assert.Equal(new byte[] { 9 }, actual.Serialized.Headers["tenant"]);
    }

    [Fact]
    public void MessageContext_Constructors_MapTopicPartitionOffset()
    {
        var fromTuple = new MessageContext(new TopicPartitionOffset("topic-a", 4, 100));
        var direct = new MessageContext("topic-b", 5, 101);

        Assert.Equal("topic-a", fromTuple.Topic);
        Assert.Equal(4, fromTuple.Partition);
        Assert.Equal(100, fromTuple.Offset);
        Assert.Equal("topic-b", direct.Topic);
        Assert.Equal(5, direct.Partition);
        Assert.Equal(101, direct.Offset);
    }

    [Fact]
    public void DeadLetterErrorContext_MapsRecordAndMetadata()
    {
        var record = new ConnectRecord("topic-a", 1, 77)
        {
            Exception = new InvalidOperationException("bad data")
        };

        var actual = new DeadLetterErrorContext(record, "orders", 3, "sync");

        Assert.Equal("topic-a", actual.Topic);
        Assert.Equal(1, actual.Partition);
        Assert.Equal(77, actual.Offset);
        Assert.Equal("orders", actual.Connector);
        Assert.Equal("03", actual.TaskId);
        Assert.Equal("sync", actual.Command);
        Assert.Same(record.Exception, actual.Exception);
        Assert.True(actual.Timestamp > 0);
    }

    [Fact]
    public void ConnectorContext_StatusAndStopped_AreDerivedFromConnectorAndTasks()
    {
        var connector = Substitute.For<IConnector>();
        var runningTask = Substitute.For<ITask>();
        var stoppedTask = Substitute.For<ITask>();
        connector.IsPaused.Returns(false);
        connector.IsStopped.Returns(false);
        runningTask.IsStopped.Returns(false);
        stoppedTask.IsStopped.Returns(true);

        var actual = new ConnectorContext
        {
            Name = "orders"
        };
        SetInternalProperty(actual, nameof(ConnectorContext.Connector), connector);

        var runningTaskContext = new TaskContext { Id = 1 };
        SetInternalProperty(runningTaskContext, nameof(TaskContext.Task), runningTask);
        var stoppedTaskContext = new TaskContext { Id = 2 };
        SetInternalProperty(stoppedTaskContext, nameof(TaskContext.Task), stoppedTask);
        actual.Tasks.Add(runningTaskContext);
        actual.Tasks.Add(stoppedTaskContext);

        Assert.Equal("Running", actual.Status);
        Assert.False(actual.IsStopped);

        connector.IsStopped.Returns(true);
        runningTask.IsStopped.Returns(true);

        Assert.True(actual.IsStopped);
    }

    [Fact]
    public void TaskContext_StatusAndStopped_AreDerivedFromTask()
    {
        var task = Substitute.For<ITask>();
        task.IsPaused.Returns(true);
        task.IsStopped.Returns(false);

        var actual = new TaskContext
        {
            Id = 7
        };
        SetInternalProperty(actual, nameof(TaskContext.Task), task);

        Assert.Equal("Paused", actual.Status);
        Assert.False(actual.IsStopped);

        task.IsPaused.Returns(false);
        task.IsStopped.Returns(true);

        Assert.Equal("Stopped", actual.Status);
        Assert.True(actual.IsStopped);
    }

    [Fact]
    public void WorkerContext_Status_UsesWorkerFirstThenLeader()
    {
        var worker = Substitute.For<IWorker>();
        worker.IsPaused.Returns(false);
        worker.IsStopped.Returns(false);

        var actual = new WorkerContext
        {
        };
        SetInternalProperty(actual, nameof(WorkerContext.Worker), worker);
        SetInternalProperty(actual, nameof(WorkerContext.Name), "worker-a");

        Assert.Equal("Running", actual.Status);
        Assert.False(actual.IsStopped);

        SetInternalProperty(actual, nameof(WorkerContext.Worker), null);
        var leader = Substitute.For<ILeader>();
        leader.IsPaused.Returns(true);
        SetInternalProperty(actual, nameof(WorkerContext.Leader), leader);

        Assert.Equal("Paused", actual.Status);
    }

    [Fact]
    public void WorkerContext_IsStopped_RequiresWorkerAndConnectorsStopped()
    {
        var worker = Substitute.For<IWorker>();
        worker.IsStopped.Returns(true);

        var runningConnector = new ConnectorContext
        {
        };
        var connector = Substitute.For<IConnector>();
        connector.IsStopped.Returns(false);
        SetInternalProperty(runningConnector, nameof(ConnectorContext.Connector), connector);

        var actual = new WorkerContext
        {
        };
        SetInternalProperty(actual, nameof(WorkerContext.Worker), worker);
        actual.Connectors.Add(runningConnector);

        Assert.False(actual.IsStopped);

        connector.IsStopped.Returns(true);
        Assert.True(actual.IsStopped);
    }

    [Fact]
    public async Task RestartContext_Retry_ReturnsFalseWhenLevelDisabled()
    {
        var context = new RestartContext(
            new RestartsConfig { EnabledFor = RestartsLevel.Task, Attempts = 1, PeriodicDelayMs = 1, RetryWaitTimeMs = 5 },
            RestartsLevel.Worker);

        var actual = await context.Retry();

        Assert.False(actual);
    }

    [Fact]
    public async Task RestartContext_Retry_ReturnsTrueThenFalseForLimitedAttempts()
    {
        var context = new RestartContext(
            new RestartsConfig { EnabledFor = RestartsLevel.Worker, Attempts = 1, PeriodicDelayMs = 1, RetryWaitTimeMs = 100 },
            RestartsLevel.Worker);

        Assert.True(await context.Retry());
        Assert.False(await context.Retry());
    }

    [Fact]
    public async Task RestartContext_Retry_ReturnsTrueForUnlimitedAttempts()
    {
        var context = new RestartContext(
            new RestartsConfig { EnabledFor = RestartsLevel.All, Attempts = -1, PeriodicDelayMs = 1, RetryWaitTimeMs = 5 },
            RestartsLevel.Task);

        var actual = await context.Retry();

        Assert.True(actual);
    }

    private static void SetInternalProperty(object target, string propertyName, object value)
    {
        var property = target.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        Assert.NotNull(property);
        property!.SetValue(target, value);
    }
}