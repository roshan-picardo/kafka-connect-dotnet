using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Connect;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect;

public class LeaderTests
{
    private readonly IConfigurationProvider _configurationProvider = Substitute.For<IConfigurationProvider>();
    private readonly IExecutionContext _executionContext = Substitute.For<IExecutionContext>();
    private readonly IKafkaClientBuilder _kafkaClientBuilder = Substitute.For<IKafkaClientBuilder>();
    private readonly IServiceScopeFactory _scopeFactory = Substitute.For<IServiceScopeFactory>();
    private readonly ILogger<Leader> _logger = Substitute.For<ILogger<Leader>>();

    [Fact]
    public async Task Execute_WhenNodeIsNotLeader_ReturnsImmediately()
    {
        _configurationProvider.IsLeader.Returns(false);
        var subject = CreateSubject();

        await subject.Execute(new CancellationTokenSource());

        _executionContext.DidNotReceive().Initialize(Arg.Any<string>(), Arg.Any<ILeader>());
    }

    [Fact]
    public async Task Execute_WhenCancelled_InitializesAndStopsLoop()
    {
        _configurationProvider.IsLeader.Returns(true);
        _configurationProvider.GetNodeName().Returns("leader-a");
        _configurationProvider.GetLeaderConfig().Returns(new LeaderConfig
        {
            Topics = new Dictionary<TopicType, string>()
        });

        var subject = CreateSubject();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await subject.Execute(cts);

        _executionContext.Received(1).Initialize("leader-a", subject);
    }

    [Fact]
    public async Task CreateInternalTopics_CreatesMissingTopicsWithExpectedSettings()
    {
        var adminClient = Substitute.For<IAdminClient>();
        adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(new Metadata(
            [new BrokerMetadata(1, "localhost", 9092)],
            new List<TopicMetadata>(),
            0,
            string.Empty));
        adminClient.CreateTopicsAsync(Arg.Any<IEnumerable<TopicSpecification>>()).Returns(Task.CompletedTask);
        _kafkaClientBuilder.GetAdminClient().Returns(adminClient);

        var subject = CreateSubject();
        var topics = new Dictionary<TopicType, string>
        {
            [TopicType.Config] = "config-topic",
            [TopicType.Command] = "command-topic"
        };

        var method = typeof(Leader).GetMethod("CreateInternalTopics", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        var task = (Task)method!.Invoke(subject, [topics])!;
        await task;

        await adminClient.Received(1).CreateTopicsAsync(Arg.Is<IEnumerable<TopicSpecification>>(specs =>
            specs.Count() == 2 &&
            specs.Any(s => s.Name == "config-topic" && s.NumPartitions == 1) &&
            specs.Any(s => s.Name == "command-topic" && s.NumPartitions == 50)));
    }

    [Fact]
    public async Task Pause_ThrowsNotImplementedException()
    {
        var subject = CreateSubject();
        await Assert.ThrowsAsync<NotImplementedException>(() => subject.Pause());
    }

    [Fact]
    public async Task Resume_ThrowsNotImplementedException()
    {
        var subject = CreateSubject();
        await Assert.ThrowsAsync<NotImplementedException>(() => subject.Resume());
    }

    private Leader CreateSubject() => new(
        _configurationProvider,
        _executionContext,
        _kafkaClientBuilder,
        _scopeFactory,
        _logger);
}
