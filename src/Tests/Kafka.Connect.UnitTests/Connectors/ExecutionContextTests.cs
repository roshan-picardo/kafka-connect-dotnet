using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using Confluent.Kafka;
using Kafka.Connect;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;
using ExecutionContext = Kafka.Connect.Connectors.ExecutionContext;

namespace UnitTests.Kafka.Connect.Connectors;

public class ExecutionContextTests
{
    private readonly IEnumerable<IPluginInitializer> _plugins;
    private readonly IEnumerable<IProcessor> _processors;
    private readonly IEnumerable<ISinkHandler> _handlers;
    private readonly IEnumerable<IMessageConverter> _messageConverters;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly ExecutionContext _executionContext;
    
    private readonly WorkerContext _workerContext;
    private int _topicPollIndex;
    private int _recordsCount;
    private readonly CancellationTokenSource _cancellationToken;
    private readonly RestartsConfig _restartsConfig;


    public ExecutionContextTests()
    {
        _plugins = Substitute.For<IEnumerable<IPluginInitializer>>();
        _processors = Substitute.For<IEnumerable<IProcessor>>();
        _handlers = Substitute.For<IEnumerable<ISinkHandler>>();
        _messageConverters = Substitute.For<IEnumerable<IMessageConverter>>();
        _configurationProvider = Substitute.For<IConfigurationProvider>();

        _executionContext =
            new ExecutionContext(_plugins, _processors, _handlers, _messageConverters,
                Substitute.For<IEnumerable<IReadWriteStrategySelector>>(), Substitute.For<IEnumerable<IWriteStrategy>>(),
                _configurationProvider);

    }

    [Theory]
    [InlineData(null, false)]
    [InlineData("worker", true)]
    [InlineData("worker", false)]
    public void Initialize_Worker(string name, bool nullWorker)
    {
        _configurationProvider.GetRestartsConfig().Returns(new RestartsConfig()
            { Attempts = 3, EnabledFor = RestartsLevel.Worker, PeriodicDelayMs = 10, RetryWaitTimeMs = 100 });
        var worker = nullWorker ? null : Substitute.For<IWorker>();

        _executionContext.Initialize(name, worker);

        var workerContext = GetPrivateWorkerContext();
        Assert.NotNull(workerContext);
        _configurationProvider.Received().GetRestartsConfig();
        Assert.Equal(workerContext.Name, name);
        Assert.Equivalent(workerContext.Worker, worker);
        Assert.Empty(workerContext.Connectors);
        Assert.NotNull(workerContext.RestartContext);
    }


    [Theory]
    [InlineData(null, true, false, false)]
    [InlineData("connector", true, false, true)]
    [InlineData("connector", false, true, true)]
    [InlineData("connector", false, false, true)]
    public void Initialize_Connector(string name, bool nullConnector,  bool updateConnector, bool expected)
    {
        _configurationProvider.GetRestartsConfig().Returns(new RestartsConfig()
            { Attempts = 3, EnabledFor = RestartsLevel.Worker, PeriodicDelayMs = 10, RetryWaitTimeMs = 100 });
        var connector = nullConnector ? null : Substitute.For<IConnector>();
        var workerContext = GetPrivateWorkerContext();
        if (updateConnector)
        {
            workerContext.Connectors.Add(new ConnectorContext() { Name = name });
        }
        
        _executionContext.Initialize(name, connector);
        
        Assert.NotNull(workerContext);
        _configurationProvider.Received(expected ? 1 : 0).GetRestartsConfig();
        Assert.Equal(workerContext.Connectors.Count, expected ? 1 : 0);
        var connectorContext = workerContext.Connectors.FirstOrDefault();
        Assert.Equal(connectorContext?.Name, name);
        Assert.Equivalent(connectorContext?.Connector, connector);
        if (expected)
            Assert.NotNull(connectorContext?.RestartContext);
        else Assert.Null(connectorContext?.RestartContext);

    }
    
    [Theory]
    [InlineData(null, 0, true, false, false)]
    [InlineData("connector", 0, true, false, false)]
    [InlineData("connector-missing", 1, true, false, false)]
    [InlineData("connector", 1, true, false, true)]
    [InlineData("connector", 1, false, false, true)]
    [InlineData("connector", 1, false, true, true)]
    public void Initialize_Task(string name, int id, bool nullTask,  bool updateTask, bool expected)
    {
        _configurationProvider.GetRestartsConfig().Returns(new RestartsConfig()
            { Attempts = 3, EnabledFor = RestartsLevel.Worker, PeriodicDelayMs = 10, RetryWaitTimeMs = 100 });
        var sinkTask = nullTask ? null : Substitute.For<ISinkTask>();
        var workerContext = GetPrivateWorkerContext();
        workerContext.Connectors.Add(updateTask
            ? new ConnectorContext { Name = "connector", Tasks = { new TaskContext { Id = id } } }
            : new ConnectorContext { Name = "connector" });

        _executionContext.Initialize(name, id, sinkTask);
        
        Assert.NotNull(workerContext);
        _configurationProvider.Received(expected ? 1 : 0).GetRestartsConfig();
        Assert.Equal(1, workerContext.Connectors.Count);
        var connectorContext = workerContext.Connectors.FirstOrDefault(c => c.Name == name);
        Assert.Equal(name == "connector" ? name : null, connectorContext?.Name);
        var taskContext = connectorContext?.Tasks?.SingleOrDefault();
        Assert.Equivalent(taskContext?.Task, sinkTask);
        Assert.Equal(name == "connector" ? id : 0, taskContext?.Id ?? 0);
        if (expected)
            Assert.NotNull(taskContext?.RestartContext);
        else Assert.Null(taskContext?.RestartContext);

    }

    [Theory]
    [InlineData(null, 0)]
    [InlineData("connector", 0)]
    [InlineData("connector-missing", 10)]
    public void AssignPartitions_TaskContextIsNull(string connector, int taskId)
    {
        var workerContext = GetPrivateWorkerContext();
        workerContext.Connectors.Add(new ConnectorContext {Name = "connector", Tasks = { new TaskContext(){ Id = 10} }});        

        _executionContext.AssignPartitions(connector, taskId, new List<TopicPartition>());

        Assert.Null(workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Tasks
            .SingleOrDefault(t => t.Id == taskId));
    }

    [Theory]
    [InlineData("topic", 4, 1)]
    [InlineData("topic", 5, 2)]
    [InlineData("topic-new", 4, 2)]
    [InlineData("topic-new", 5, 2)]
    public void AssignPartitions_AddsOrUpdatesNewPartition(string topic, int partition, int expectedCount)
    {
        var assignments = new List<AssignmentContext>() { new() { Topic = "topic", Partition = 4 } };
        var workerContext = GetPrivateWorkerContext();
        workerContext.Connectors.Add(new ConnectorContext
        {
            Name = "connector",
            Tasks = { new TaskContext() { Id = 10, Assignments = assignments } }
        });

        _executionContext.AssignPartitions("connector", 10, new List<TopicPartition>() { new(topic, partition) });

        Assert.Equal(assignments.Count, expectedCount);
        Assert.Contains(assignments, tp => tp.Topic == topic && tp.Partition == partition);
    }
    
    [Theory]
    [InlineData(null, 0)]
    [InlineData("connector", 0)]
    [InlineData("connector-missing", 10)]
    public void RevokePartitions_TaskContextIsNull(string connector, int taskId)
    {
        var workerContext = GetPrivateWorkerContext();
        workerContext.Connectors.Add(new ConnectorContext {Name = "connector", Tasks = { new TaskContext(){ Id = 10} }});        

        _executionContext.RevokePartitions(connector, taskId, new List<TopicPartition>());

        Assert.Null(workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Tasks
            .SingleOrDefault(t => t.Id == taskId));
    }
    
    [Theory]
    [InlineData("topic", 4, 0)]
    [InlineData("topic", 5, 1)]
    [InlineData("topic-new", 4, 1)]
    [InlineData("topic-new", 5, 1)]
    public void AssignPartitions_RemovesOrSkipsNewPartition(string topic, int partition, int expectedCount)
    {
        var assignments = new List<AssignmentContext>() { new() { Topic = "topic", Partition = 4 } };;
        var workerContext = GetPrivateWorkerContext();
        workerContext.Connectors.Add(new ConnectorContext
        {
            Name = "connector",
            Tasks = { new TaskContext() { Id = 10, Assignments = assignments } }
        });

        _executionContext.RevokePartitions("connector", 10, new List<TopicPartition>() { new(topic, partition) });

        Assert.Equal(assignments.Count, expectedCount);
        Assert.DoesNotContain(assignments, tp => tp.Topic == topic && tp.Partition == partition);
    }

    [Theory]
    [InlineData("connector-new", 10)]
    [InlineData("connector", 12)]
    [InlineData("connector-new", 12)]
    public void GetOrSetBatchContext_WhenConnectorOrTaskIsNull(string connector, int taskId)
    {
        var workerContext = GetPrivateWorkerContext();
        var taskContext = new TaskContext(){ Id = 10};
        workerContext.Connectors.Add(new ConnectorContext {Name = "connector", Tasks = { taskContext }});
        var cts = new CancellationTokenSource();

        var actual = _executionContext.GetOrSetBatchContext(connector, taskId, cts.Token);
        Assert.NotNull(actual);
        Assert.Equal(actual.Token, cts.Token);
        Assert.Null(taskContext.BatchContext);
    }
    
    [Fact]
    public void GetOrSetBatchContext_WhenTaskExists()
    {
        var workerContext = GetPrivateWorkerContext();
        var taskContext = new TaskContext(){ Id = 10};
        workerContext.Connectors.Add(new ConnectorContext {Name = "connector", Tasks = { taskContext }});
        var cts = new CancellationTokenSource();

        var actual = _executionContext.GetOrSetBatchContext("connector", 10, cts.Token);
        Assert.NotNull(actual);
        Assert.Equal(actual.Token, cts.Token);
        Assert.NotNull(taskContext.BatchContext);
        Assert.Equal(taskContext.BatchContext.Token, cts.Token);
    }
    
    [Fact]
    public void GetOrSetBatchContext_WhenBatchContextAlreadyExists()
    {
        var workerContext = GetPrivateWorkerContext();
        var taskContext = new TaskContext(){ Id = 10, BatchContext = new BatchPollContext() { Token = new CancellationToken()}};
        workerContext.Connectors.Add(new ConnectorContext {Name = "connector", Tasks = { taskContext }});
        var cts = new CancellationTokenSource();

        var actual = _executionContext.GetOrSetBatchContext("connector", 10, cts.Token);
        
        Assert.NotNull(actual);
        Assert.NotNull(taskContext.BatchContext);
        // Shouldn't override the token
        Assert.NotEqual(actual.Token, cts.Token);
        Assert.NotEqual(taskContext.BatchContext.Token, cts.Token);
    }

    [Fact]
    public void GetNextPollIndex()
    {
        var expected = GetOrSetPrivateIntegers("_topicPollIndex", 30) + 1;

        var actual = _executionContext.GetNextPollIndex();
        Assert.Equal(expected, actual);
    }
    
    [Fact]
    public void AddToCount()
    {
        GetOrSetPrivateIntegers("_recordsCount", 30);

        _executionContext.AddToCount(20);
        
        Assert.Equal(GetOrSetPrivateIntegers("_recordsCount"), 50);
    }

    [Theory]
    [InlineData(null, 0, true, true, true, RestartsLevel.None)]
    [InlineData(null, 0, false, true, true, RestartsLevel.Worker)]
    [InlineData("connector", 0, false, true, true, RestartsLevel.None)]
    [InlineData("connector-one", 0, false, false, true, RestartsLevel.None)]
    [InlineData("connector", 0, false, false, true, RestartsLevel.Connector)]
    [InlineData("connector", 1, false, false, true, RestartsLevel.None)]
    [InlineData("connector", 2, false, false, false, RestartsLevel.None)] 
    [InlineData("connector", 1, false, false, false, RestartsLevel.None)] // Must be Task
    public void Pause_WorkerOrConnectorOrTask(string name, int id, bool nullWorker, bool nullConnector, bool nullTask, RestartsLevel expected)
    {
        var sinkTask = nullTask ? null : Substitute.For<ISinkTask>();
        var connector = nullConnector ? null : Substitute.For<IConnector>();
        var worker = nullWorker ? null : Substitute.For<IWorker>();
        _executionContext.Initialize("worker", worker);
        _executionContext.Initialize("connector", connector);
        _executionContext.Initialize("connector", 1, sinkTask);
        
        _executionContext.Pause(name, id);

        worker?.Received(expected == RestartsLevel.Worker ? 1 : 0).Pause();
        connector?.Received(expected == RestartsLevel.Connector ? 1 : 0).Pause();
        //sinkTask.Received(expected == RestartsLevel.Task ? 1 : 0).Pause();
    }
    
    [Theory]
    [InlineData(null, 0, true, true, true, RestartsLevel.None)]
    [InlineData(null, 0, false, true, true, RestartsLevel.Worker)]
    [InlineData("connector", 0, false, true, true, RestartsLevel.None)]
    [InlineData("connector-one", 0, false, false, true, RestartsLevel.None)]
    [InlineData("connector", 0, false, false, true, RestartsLevel.Connector)]
    [InlineData("connector", 1, false, false, true, RestartsLevel.None)]
    [InlineData("connector", 2, false, false, false, RestartsLevel.None)] 
    [InlineData("connector", 1, false, false, false, RestartsLevel.None)] // Must be Task
    public void Resume_WorkerOrConnectorOrTask(string name, int id, bool nullWorker, bool nullConnector, bool nullTask, RestartsLevel expected)
    {
        var sinkTask = nullTask ? null : Substitute.For<ISinkTask>();
        var connector = nullConnector ? null : Substitute.For<IConnector>();
        var worker = nullWorker ? null : Substitute.For<IWorker>();
        _executionContext.Initialize("worker", worker);
        _executionContext.Initialize("connector", connector);
        _executionContext.Initialize("connector", 1, sinkTask);
        
        _executionContext.Resume(name, id);

        worker?.Received(expected == RestartsLevel.Worker ? 1 : 0).Resume();
        connector?.Received(expected == RestartsLevel.Connector ? 1 : 0).Resume(Arg.Any<IDictionary<string, string>>());
        //sinkTask.Received(expected == RestartsLevel.Task ? 1 : 0).Resume();
    }


    [Theory]
    [InlineData(null, true, false)]
    [InlineData("connector-other", true,  false)]
    [InlineData("connector", true, false)]
    [InlineData("connector", false, true)]
    public void GetConnector_Tests(string name, bool nullConnector, bool expectedNotNull)
    {
        var connector = nullConnector ? null : Substitute.For<IConnector>();
        var context = GetPrivateWorkerContext();
        context.Connectors.Add(new ConnectorContext{Name = "connector"});
        _executionContext.Initialize("connector", connector);

        var actual = _executionContext.GetConnector(name);
        
        if(expectedNotNull) Assert.NotNull(actual);
        else Assert.Null(actual);
    }
    
    [Theory]
    [InlineData(null, 0, true, false)]
    [InlineData("connector-other", 0, true,  false)]
    [InlineData("connector", 0, true,  false)]
    [InlineData("connector", 1, true, false)]
    [InlineData("connector", 1, false, true)]
    public void GetSinkTask_Tests(string name, int id, bool nullTask, bool expectedNotNull)
    {
        var sinkTask = nullTask ? null : Substitute.For<ISinkTask>();
        var context = GetPrivateWorkerContext();
        context.Connectors.Add(new ConnectorContext{Name = "connector"});
        _executionContext.Initialize("connector", 1, sinkTask);

        var actual = _executionContext.GetSinkTask(name, 1);
        
        if(expectedNotNull) Assert.NotNull(actual);
        else Assert.Null(actual);
    }
    
    

    private WorkerContext GetPrivateWorkerContext()
    {
        var workerContext = typeof(ExecutionContext)
            .GetField("_workerContext", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(_executionContext);
        return workerContext as WorkerContext;
    }

    private int? GetOrSetPrivateIntegers(string field, int value = 0)
    {
        if (value != 0)
        {
            typeof(ExecutionContext)
                .GetField(field, BindingFlags.NonPublic | BindingFlags.Instance)?.SetValue(_executionContext, value);
        }

        return typeof(ExecutionContext)
            .GetField(field, BindingFlags.NonPublic | BindingFlags.Instance)
            ?.GetValue(_executionContext) as int?;
    }
}