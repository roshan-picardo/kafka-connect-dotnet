using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Connectors;

public class ExecutionContextTests
{
    private readonly IConfigurationProvider _configurationProvider = Substitute.For<IConfigurationProvider>();

    [Fact]
    public void AssignAndRevokePartitions_TracksAssignmentsPerTask()
    {
        var subject = CreateSubject();
        var worker = Substitute.For<IWorker>();
        var connector = Substitute.For<IConnector>();
        var task = Substitute.For<ITask>();

        _configurationProvider.GetRestartsConfig().Returns(new RestartsConfig
        {
            EnabledFor = RestartsLevel.All,
            Attempts = 1,
            RetryWaitTimeMs = 1,
            PeriodicDelayMs = 1
        });

        subject.Initialize("worker-a", worker);
        subject.Initialize("orders", connector);
        subject.Initialize("orders", 1, task);

        subject.AssignPartitions("orders", 1,
        [
            new TopicPartition("topic-a", new Partition(0)),
            new TopicPartition("topic-a", new Partition(1))
        ]);

        var assigned = subject.GetAssignedPartitions("orders", 1);
        Assert.True(assigned.ContainsKey("topic-a"));
        Assert.Equal(2, assigned["topic-a"].Count);

        subject.RevokePartitions("orders", 1, [new TopicPartition("topic-a", new Partition(0))]);

        var afterRevoke = subject.GetAssignedPartitions("orders", 1);
        Assert.Single(afterRevoke["topic-a"]);
        Assert.Equal(1, afterRevoke["topic-a"].Single());
    }

    [Fact]
    public async Task Refresh_WithDelete_MarksConnectorDeletedInSimpleStatus()
    {
        var subject = CreateSubject();
        var worker = Substitute.For<IWorker>();
        var connector = Substitute.For<IConnector>();

        _configurationProvider.GetRestartsConfig().Returns(new RestartsConfig
        {
            EnabledFor = RestartsLevel.All,
            Attempts = 1,
            RetryWaitTimeMs = 1,
            PeriodicDelayMs = 1
        });

        subject.Initialize("worker-a", worker);
        subject.Initialize("orders", connector);

        await subject.Refresh("orders", isDelete: true);

        await worker.Received(1).Refresh("orders", true);

        var simple = subject.GetSimpleStatus();
        var json = JsonSerializer.SerializeToNode(simple)?.AsObject();
        var connectors = json?["Connectors"]?.AsArray();
        Assert.NotNull(connectors);
        Assert.Empty(connectors!);
    }

    [Fact]
    public void UpdateCommandsAndLeaderAssignments_ReplaceTaskAssignments()
    {
        var subject = CreateSubject();
        var worker = Substitute.For<IWorker>();
        var connector = Substitute.For<IConnector>();
        var task = Substitute.For<ITask>();

        _configurationProvider.GetRestartsConfig().Returns(new RestartsConfig
        {
            EnabledFor = RestartsLevel.All,
            Attempts = 1,
            RetryWaitTimeMs = 1,
            PeriodicDelayMs = 1
        });
        _configurationProvider.GetTopic(TopicType.Config).Returns("config-topic");

        subject.Initialize("worker-a", worker);
        subject.Initialize("orders", connector);
        subject.Initialize("orders", 1, task);

        subject.UpdateCommands("orders", 1,
        [
            new CommandRecord { Name = "c1", Topic = "command-topic", Partition = 2 },
            new CommandRecord { Name = "c2", Topic = "command-topic", Partition = 3 }
        ]);

        var commandAssigned = subject.GetAssignedPartitions("orders", 1);
        Assert.True(commandAssigned.ContainsKey("command-topic"));
        Assert.Equal(2, commandAssigned["command-topic"].Count);

        subject.UpdateLeaderAssignments("orders", 1, TopicType.Config);
        var leaderAssigned = subject.GetAssignedPartitions("orders", 1);

        Assert.True(leaderAssigned.ContainsKey("config-topic"));
        Assert.Single(leaderAssigned["config-topic"]);
        Assert.Equal(0, leaderAssigned["config-topic"].Single());
    }

    [Fact]
    public void SetPartitionEof_UpdatesAllPartitionEofState()
    {
        var subject = CreateSubject();
        var worker = Substitute.For<IWorker>();
        var connector = Substitute.For<IConnector>();
        var task = Substitute.For<ITask>();

        _configurationProvider.GetRestartsConfig().Returns(new RestartsConfig
        {
            EnabledFor = RestartsLevel.All,
            Attempts = 1,
            RetryWaitTimeMs = 1,
            PeriodicDelayMs = 1
        });

        subject.Initialize("worker-a", worker);
        subject.Initialize("orders", connector);
        subject.Initialize("orders", 1, task);
        subject.AssignPartitions("orders", 1, [new TopicPartition("topic-a", new Partition(0))]);

        Assert.False(subject.AllPartitionEof("orders", 1));

        subject.SetPartitionEof("orders", 1, "topic-a", 0, true);

        Assert.True(subject.AllPartitionEof("orders", 1));
    }

    private ExecutionContext CreateSubject() => new(
        Substitute.For<IEnumerable<IPluginInitializer>>(),
        Substitute.For<IEnumerable<IProcessor>>(),
        Substitute.For<IEnumerable<ISinkHandler>>(),
        Substitute.For<IEnumerable<IMessageConverter>>(),
        Substitute.For<IEnumerable<IStrategySelector>>(),
        Substitute.For<IEnumerable<IStrategy>>(),
        _configurationProvider);
}
