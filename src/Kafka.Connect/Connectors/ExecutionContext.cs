using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Connectors;

public class ExecutionContext(
    IEnumerable<IPluginInitializer> plugins,
    IEnumerable<IProcessor> processors,
    IEnumerable<ISinkHandler> handlers,
    IEnumerable<IMessageConverter> messageConverters,
    IEnumerable<IStrategySelector> strategySelectors,
    IEnumerable<IStrategy> queryStrategies,
    IConfigurationProvider configurationProvider)
    : IExecutionContext
{
    private readonly WorkerContext _workerContext = new();
    private int _recordsCount;
    private readonly CancellationTokenSource _cancellationToken = new();

    public void Initialize(string name, IWorker worker)
    {
        _workerContext.Name = name;
        _workerContext.Worker = worker;
        _workerContext.RestartContext =
            new RestartContext(configurationProvider.GetRestartsConfig(), RestartsLevel.Worker);
        _workerContext.Connectors.Clear();
    }
    
    public void Initialize(string name, ILeader leader)
    {
        _workerContext.Name = name;
        _workerContext.Leader = leader;
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
            new RestartContext(configurationProvider.GetRestartsConfig(), RestartsLevel.Connector);
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
            new RestartContext(configurationProvider.GetRestartsConfig(), RestartsLevel.Task);
        taskContext.Assignments.Clear();
    }

    public void AssignPartitions(string connector, int task, IEnumerable<TopicPartition> partitions)
    {
        var taskContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Tasks
            .SingleOrDefault(t => t.Id == task);
        if(taskContext == null) return;
        foreach (var partition in partitions)
        {
            if (!taskContext.Assignments.Any(a=> a.Topic == partition.Topic && a.Partition == partition.Partition.Value))
            {
                taskContext.Assignments.Add(new AssignmentContext { Topic = partition.Topic, Partition = partition.Partition.Value });
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
            var assignment = taskContext.Assignments.SingleOrDefault(a => a.Topic == partition.Topic && a.Partition == partition.Partition.Value);
            if (assignment != null)
            {
                taskContext.Assignments.Remove(assignment);
            }
        }
    }

    public IDictionary<string, List<int>> GetAssignedPartitions(string connector, int task)
    {
        var taskContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Tasks
            .SingleOrDefault(t => t.Id == task);
        if (taskContext == null) return new Dictionary<string, List<int>>();
        return taskContext.Assignments.GroupBy(a => a.Topic)
            .ToDictionary(g => g.Key, g => g.Select(a => a.Partition).ToList());
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
            Plugins = plugins?.Select(p => p?.GetType().Assembly.GetName().Name),
            Initializers = plugins?.Select(p => p?.GetType().FullName),
            Processors = processors?.Select(p => p?.GetType().FullName),
            Converters = messageConverters?.Select(d => d?.GetType().FullName),
            Handlers = handlers?.Select(h => h?.GetType().FullName),
            Strategies = new
            {
                Selectors = strategySelectors?.Select(s => s?.GetType().FullName),
                Builders = queryStrategies?.Select(s => s?.GetType().FullName)
            }
        };
    }

    public bool IsStopped => _workerContext.IsStopped;

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
        if (_cancellationToken is { IsCancellationRequested: false })
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

    public void SetPartitionEof(string connector, int task, string topic, int partition, bool eof)
    {
        var taskContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Tasks
            .SingleOrDefault(t => t.Id == task);
        var assignment = taskContext?.Assignments?.SingleOrDefault(a => a.Topic == topic && a.Partition == partition);
        if (assignment != null)
        {
            assignment.IsEof = eof;
        }
    }

    public bool AllPartitionEof(string connector, int task)
    {
        var taskContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Tasks
            .SingleOrDefault(t => t.Id == task);
        return taskContext?.Assignments?.All(a => a.IsEof) ?? true;
    }

    public void UpdateCommands(string connector, int task, IEnumerable<CommandRecord> tasks)
    {
        var taskContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector)?.Tasks
            .SingleOrDefault(t => t.Id == task);
        if(taskContext == null) return;
        taskContext.Assignments.Clear();
        foreach (var command in tasks)
        {
            var assignment = taskContext.Assignments.SingleOrDefault(a => a.Name == command.Name);
            if (assignment == null)
            {
                assignment = new AssignmentContext { Name = command.Name, Partition = command.Partition, Topic = command.Topic};
                taskContext.Assignments.Add(assignment);
            }

            assignment.Partition = command.Partition;
            assignment.Topic = command.Topic;
        }
    }

    private static dynamic GetTaskStatus(TaskContext taskContext) => new
    {
        Id = taskContext.Id.ToString("00"),
        taskContext.Status,
        Uptime = taskContext.Uptime.ToString(@"dd\.hh\:mm\:ss"),
        Assignments = taskContext.Task switch
        {
            ISinkTask => taskContext.Assignments.Select(a => new { a.Topic, a.Partition }) as dynamic,
            ISourceTask => taskContext.Assignments.Select(a => new { a.Name, a.Topic, a.Partition }),
            _ => null
        }
    };
    
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
                Assigned = connectorContext.Tasks.Count(t => t.Assignments != null && t.Assignments.Any())
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
                Records = _recordsCount
            },
            Connectors = _workerContext.Connectors.Select(c => GetConnectorStatus(c))
        };
    }
}