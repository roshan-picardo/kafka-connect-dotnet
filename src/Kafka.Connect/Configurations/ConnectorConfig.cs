using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Configurations
{
    public class ConnectorConfig 
    {
        private readonly IList<string> _topics = new List<string>();
        private readonly string _groupId;
        private readonly string _clientId;
        private string _topic;
        private readonly IDictionary<string, ProcessorConfig> _processors;
        private readonly LogConfig _log;

        public string Name { get; set; }
        public ConnectorType Type { get; set; }

        public string GroupId
        {
            get => _groupId ?? Name;
            init => _groupId = value;
        }
        public bool Disabled { get; init; }

        public string Topic
        {
            get
            {
                if (Type == ConnectorType.Source && string.IsNullOrWhiteSpace(_topic))
                {
                    _topic = _topics.FirstOrDefault();
                }

                return _topic;
            }
            init => _topic = value;
        }

        public IList<string> Topics
        {
            get
            {
                if (Type == ConnectorType.Sink && !string.IsNullOrWhiteSpace(_topic) && !_topics.Contains(_topic))
                {
                    _topics.Add(_topic);
                }

                return _topics;
            }
            init => _topics = value?.ToList() ?? new List<string>();
        }
        public int MaxTasks { get; init; }
        public bool Paused { get; init; }
        public PluginConfig Plugin { get; set; }

        public LogConfig Log
        {
            get => _log ?? new LogConfig { Provider = typeof(DefaultLogRecord).FullName };
            init => _log = value;
        }

        public RetryConfig Retries { get; init; }
        public BatchConfig Batches { get; init; }
        public IDictionary<string, ProcessorConfig> Processors {
            get
            {
                if (_processors == null || !_processors.Any())
                {
                    return _processors;
                }
                foreach (var (name, processor) in _processors)
                {
                    if (processor != null && string.IsNullOrEmpty(processor.Name))
                    {
                        processor.Name = name;
                    }
                }
                return _processors;
            }
            init => _processors = value;
        }

        public string ClientId
        {
            get => _clientId ?? Name;
            init => _clientId = value;
        }
    }

    public enum ConnectorType
    {
        Leader,
        Worker,
        Sink,
        Source
    }
}