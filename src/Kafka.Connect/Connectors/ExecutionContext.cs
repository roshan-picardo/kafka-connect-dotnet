using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Connectors;

public class ExecutionContext : IExecutionContext
{
    private readonly IEnumerable<IPluginInitializer> _plugins;
    private readonly IEnumerable<IProcessor> _processors;
    private readonly IEnumerable<ISinkHandler> _handlers;
    private readonly IEnumerable<IDeserializer> _deserializers;
    private readonly IEnumerable<IWriteStrategySelector> _strategySelectors;
    private readonly IEnumerable<IWriteStrategy> _writeStrategies;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly WorkerContext _workerContext;
    private int _topicPollIndex;
    private int _recordsCount;
    private readonly CancellationTokenSource _cancellationToken;

    public ExecutionContext(
        IEnumerable<IPluginInitializer> plugins, 
        IEnumerable<IProcessor> processors,
        IEnumerable<ISinkHandler> handlers, 
        IEnumerable<IDeserializer> deserializers, 
        IEnumerable<IWriteStrategySelector> strategySelectors,
        IEnumerable<IWriteStrategy> writeStrategies,
        IConfigurationProvider configurationProvider)
    {
        _plugins = plugins;
        _processors = processors;
        _handlers = handlers;
        _deserializers = deserializers;
        _strategySelectors = strategySelectors;
        _writeStrategies = writeStrategies;
        _configurationProvider = configurationProvider;
        _workerContext = new WorkerContext();
        _topicPollIndex = 0;
        _recordsCount = 0;
        _cancellationToken = new CancellationTokenSource();
    }

    public void Initialize(string name, IWorker worker)
    {
        _workerContext.Name = name;
        _workerContext.Worker = worker;
        _workerContext.RestartContext =
            new RestartContext(_configurationProvider.GetRestartsConfig(), RestartsLevel.Worker);
        _workerContext.Connectors.Clear();
    }

    public void Initialize(string name, IConnector connector)
    {
        if (string.IsNullOrWhiteSpace(name)) return;
        var context = _workerContext.Connectors.SingleOrDefault(c => c.Name == name);
        if (context == null)
        {
            context = new ConnectorContext { Name = name };
            _workerContext.Connectors.Add(context);
        }
        context.Connector = connector;
        context.RestartContext =
            new RestartContext(_configurationProvider.GetRestartsConfig(), RestartsLevel.Connector);
        context.Tasks.Clear();
    }

    public void Initialize(string connector, int taskId, ITask task)
    {
        if(string.IsNullOrWhiteSpace(connector) || taskId <= 0) return;
        var connectorContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector);
        if(connectorContext == null) return;
        var taskContext = connectorContext.Tasks.SingleOrDefault(t => t.Id == taskId);
        if (taskContext == null)
        {
            taskContext = new TaskContext { Id = taskId };
            connectorContext.Tasks.Add(taskContext);
        }
        taskContext.Task = task;
        taskContext.RestartContext =
            new RestartContext(_configurationProvider.GetRestartsConfig(), RestartsLevel.Task);
        taskContext.TopicPartitions.Clear();
    }

    public void AssignPartitions(string connector, int task, IEnumerable<TopicPartition> partitions)
    {
        var taskContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Tasks
            .SingleOrDefault(t => t.Id == task);
        if(taskContext == null) return;
        foreach (var partition in partitions)
        {
            if (!taskContext.TopicPartitions.Contains((partition.Topic, partition.Partition.Value)))
            {
                taskContext.TopicPartitions.Add((partition.Topic, partition.Partition.Value));
            }
        }
    }
        
    public void RevokePartitions(string connector, int task, IEnumerable<TopicPartition> partitions)
    {
        var taskContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Tasks
            .SingleOrDefault(t => t.Id == task);
        if(taskContext == null) return;
        foreach (var partition in partitions)
        {
            if (taskContext.TopicPartitions.Contains((partition.Topic, partition.Partition.Value)))
            {
                taskContext.TopicPartitions.Remove((partition.Topic, partition.Partition.Value));
            }
        }
    }

    public dynamic GetStatus(string connector = null, int task = 0)
    {
        var connectorContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector);
        if (connectorContext == null) return GetWorkerStatus();
        var taskContext = connectorContext.Tasks.SingleOrDefault(t => t.Id == task);
        return taskContext != null ? GetTaskStatus(taskContext) : GetConnectorStatus(connectorContext);
    }

    public dynamic GetFullDetails()
    {
        return new
        {
            Worker = GetWorkerStatus(),
            Plugins = _plugins?.Select(p => p?.GetType().Assembly.GetName().Name),
            Initializers = _plugins?.Select(p => p?.GetType().FullName),
            Processors = _processors?.Select(p => p?.GetType().FullName),
            Deserializers = _deserializers?.Select(d => d?.GetType().FullName),
            Handlers = _handlers?.Select(h => h?.GetType().FullName),
            Writers = new
            {
                Selectors = _strategySelectors?.Select(s => s?.GetType().FullName),
                Strategies = _writeStrategies?.Select(s => s?.GetType().FullName)
            }
        };
    }

    public bool IsStopped => _workerContext.IsStopped;

    public BatchPollContext GetOrSetBatchContext(string connector, int taskId, CancellationToken token = default)
    {
        var taskContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Tasks
            .SingleOrDefault(t => t.Id == taskId);
        if (taskContext == null) return new BatchPollContext {Token = token};
        taskContext.BatchContext ??= new BatchPollContext {Token = token};
        return taskContext.BatchContext;
    }

    public int GetNextPollIndex()
    {
        return Interlocked.Increment(ref _topicPollIndex);
    }

    public void AddToCount(int records)
    {
        Interlocked.Add(ref _recordsCount, records);
    }

    public CancellationTokenSource GetToken()
    {
        return _cancellationToken;
    }

    public void Shutdown()
    {
        if (_cancellationToken is {IsCancellationRequested: false})
        {
            _cancellationToken.Cancel();
        }
    }

    public void Pause(string connector = null, int task = 0)
    {
        if (string.IsNullOrWhiteSpace(connector))
        { 
            _workerContext.Worker?.Pause();
        }
        else if (task <= 0)
        {
            _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Connector?.Pause();
        }
        else
        {
            // TODO: Pause the Task
        }
    }

    public void Resume(string connector = null, int task = 0)
    {
        if (string.IsNullOrWhiteSpace(connector))
        {
            _workerContext.Worker?.Resume();
        }
        else if (task <= 0)
        {
            _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Connector?.Resume(null);
        }
        else
        {
            // TODO: Resume the Task
        }
    }

    public async Task Restart(int delay, string connector = null, int task = 0)
    {
        //TODO: this method needs to cancel the token and call Execute method 
        Pause(connector, task);
        await Task.Delay(delay);
        Resume(connector, task);
    }

    public IConnector GetConnector(string connector) =>
        _workerContext.Connectors?.SingleOrDefault(c => c.Name == connector)?.Connector;


    public ITask GetSinkTask(string connector, int task) => _workerContext.Connectors
        ?.SingleOrDefault(c => c.Name == connector)?.Tasks?.SingleOrDefault(t => t.Id == task)?.Task;

    public async Task<bool> Retry(string connector = null, int task = 0)
    {
        if (string.IsNullOrWhiteSpace(connector))
        {
            return await _workerContext.RestartContext.Retry();
        }

        if(task <= 0)
        {
            return await _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.RestartContext
                .Retry()!;
        }

        return await _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Tasks
            ?.SingleOrDefault(t => t.Id == task)?.RestartContext.Retry()!;
    }

    private static dynamic GetTaskStatus(TaskContext taskContext)
    {
        return new
        {
            Id = taskContext.Id.ToString("00"),
            taskContext.Status,
            Uptime = taskContext.Uptime.ToString(@"dd\.hh\:mm\:ss"),
            Assignments = taskContext.TopicPartitions.Select(p =>
            {
                var (topic, partition) = p;
                return new
                {
                    Topic = topic,
                    Partition = partition
                };
            })
        };
    }
    private static dynamic GetConnectorStatus(ConnectorContext connectorContext)
    {
        return new
        {
            connectorContext.Name,
            connectorContext.Status,
            Uptime = connectorContext.Uptime.ToString(@"dd\.hh\:mm\:ss"),
            Summary = new
            {
                connectorContext.Tasks.Count,
                Running = connectorContext.Tasks.Count(t => !t.Task.IsPaused && !t.IsStopped),
                Paused = connectorContext.Tasks.Count(t => t.Task.IsPaused),
                Stopped = connectorContext.Tasks.Count(t => !t.Task.IsPaused && t.IsStopped),
                Assigned = connectorContext.Tasks.Count(t => t.TopicPartitions != null && t.TopicPartitions.Any())
            },
            Tasks = connectorContext.Tasks.Select(t => GetTaskStatus(t))
        };
    }
    private dynamic GetWorkerStatus()
    {
        return new
        {
            _workerContext.Name,
            _workerContext.Status,
            Uptime = _workerContext.Uptime.ToString(@"dd\.hh\:mm\:ss"),
            Summary = new
            {
                _workerContext.Connectors.Count,
                Running = _workerContext.Connectors.Count(c => !c.Connector.IsPaused && !c.IsStopped),
                Paused = _workerContext.Connectors.Count(c => c.Connector.IsPaused),
                Stopped = _workerContext.Connectors.Count(c => !c.Connector.IsPaused && c.IsStopped),
                Poll = _topicPollIndex,
                Records = _recordsCount
            },
            Connectors = _workerContext.Connectors.Select(c => GetConnectorStatus(c))
        };
    }
}