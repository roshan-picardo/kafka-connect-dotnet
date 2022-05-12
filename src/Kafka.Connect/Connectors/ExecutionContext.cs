using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Serializers;

namespace Kafka.Connect.Connectors
{
    public class ExecutionContext : IExecutionContext
    {
        private readonly IEnumerable<IPluginInitializer> _plugins;
        private readonly IEnumerable<IProcessor> _processors;
        private readonly IEnumerable<ISinkHandler> _handlers;
        private readonly IEnumerable<IDeserializer> _deserializers;
        private readonly WorkerContext _workerContext;
        private int _topicPollIndex;
        private int _recordsCount;
        private readonly CancellationTokenSource _cancellationToken;

        public ExecutionContext(IEnumerable<IPluginInitializer> plugins, IEnumerable<IProcessor> processors,
            IEnumerable<ISinkHandler> handlers, IEnumerable<IDeserializer> deserializers)
        {
            _plugins = plugins;
            _processors = processors;
            _handlers = handlers;
            _deserializers = deserializers;
            _workerContext = new WorkerContext
            {
                Status = Status.Running,
                Connectors = new List<ConnectorContext>()
            };
            _topicPollIndex = 0;
            _recordsCount = 0;
            _cancellationToken = new CancellationTokenSource();
        }

        public void Name(string worker) => _workerContext.Name = worker;

        public T GetOrAdd<T>(string connector = null, int task = 0)
        {
            object context = default(T);
            var connectorContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector);
            if (!string.IsNullOrEmpty(connector) && connectorContext == null)
            {
                connectorContext = new ConnectorContext {Name = connector};
                _workerContext.Connectors.Add(connectorContext);
                context = connectorContext;
            }
            else if(connectorContext != null)
            {
                var taskContext = connectorContext.Tasks.SingleOrDefault(t => t.Id == task);
                if (task <= 0 || taskContext != null)
                {
                    context = connectorContext;
                }
                else
                {
                    taskContext = new TaskContext {Id = task};
                    connectorContext.Tasks.Add(taskContext);
                    context = taskContext;
                }
            }

            return (T) context;
        }

        public void Add(string connector = null, int task = 0)
        {
            var connectorContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector);
            if (!string.IsNullOrEmpty(connector) && connectorContext == null)
            {
                connectorContext = new ConnectorContext {Name = connector, Tasks = new List<TaskContext>()};
                _workerContext.Connectors.Add(connectorContext);
            }
            else if(connectorContext != null)
            {
                var taskContext = connectorContext.Tasks.SingleOrDefault(t => t.Id == task);
                if (task <= 0 || taskContext != null) return;
                taskContext = new TaskContext {Id = task, TopicPartitions = new List<(string, int)>()};
                connectorContext.Tasks.Add(taskContext);
            }
        }

        public void Pause(string connector = null, int task = 0) => UpdateStatus(Status.Paused, connector, task);
        public void Stop(string connector = null, int task = 0) => UpdateStatus(Status.Stopped, connector, task);
        public void Start(string connector = null, int task = 0) => UpdateStatus(Status.Running, connector, task);

        public void Clear(string connector = null)
        {
            var connectorContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector);
            if (connectorContext != null)
            {
                connectorContext.Tasks.Clear();
            }
            else
            {
                _workerContext.Connectors.Clear();
            }
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
                Plugins = _plugins?.Select(p => p?.GetType().FullName),
                Processors = _processors?.Select(p => p?.GetType().FullName),
                Deserializers = _deserializers?.Select(d => d?.GetType().FullName),
                Handlers = _handlers?.Select(h => h?.GetType().FullName)
            };
        }

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

        private dynamic GetTaskStatus(TaskContext taskContext)
        {
            return new
            {
                Id = taskContext.Id.ToString("00"),
                Status = taskContext.Status.ToString(),
                Uptime = taskContext.Uptime.ToString(@"dd\.hh\:mm\:ss"),
                Assignment = taskContext.TopicPartitions.Select(p =>
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
        private dynamic GetConnectorStatus(ConnectorContext connectorContext)
        {
            return new
            {
                connectorContext.Name,
                Status = connectorContext.Status.ToString(),
                Uptime = connectorContext.Uptime.ToString(@"dd\.hh\:mm\:ss"),
                Summary = new
                {
                    connectorContext.Tasks.Count,
                    Running = connectorContext.Tasks.Count(t => t.Status == Status.Running),
                    Paused = connectorContext.Tasks.Count(t => t.Status == Status.Paused),
                    Stopped = connectorContext.Tasks.Count(t => t.Status == Status.Stopped),
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
                Status = _workerContext.Status.ToString(),
                Uptime = _workerContext.Uptime.ToString(@"dd\.hh\:mm\:ss"),
                Summary = new
                {
                    _workerContext.Connectors.Count,
                    Running = _workerContext.Connectors.Count(c => c.Status == Status.Running),
                    Paused = _workerContext.Connectors.Count(c => c.Status == Status.Paused),
                    Stopped = _workerContext.Connectors.Count(c => c.Status == Status.Stopped),
                    Poll = _topicPollIndex,
                    Records = _recordsCount
                },
                Connectors = _workerContext.Connectors.Select(c => GetConnectorStatus(c))
            };
        }

        private void UpdateStatus(Status status, string connector, int task)
        {
            var connectorContext = _workerContext.Connectors.SingleOrDefault(c => c.Name == connector);
            if (connectorContext != null)
            {
                var taskContext = connectorContext.Tasks.SingleOrDefault(t => t.Id == task);
                if (taskContext != null)
                {
                    taskContext.Status = status;
                }
                else
                {
                    connectorContext.Status = status;
                }
            }
            else
            {
                _workerContext.Status = status;
            }
        }
    }
}