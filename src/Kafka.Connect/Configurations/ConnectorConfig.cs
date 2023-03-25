using System.Collections.Generic;
using System.Linq;

namespace Kafka.Connect.Configurations
{
    public class ConnectorConfig 
    {
        private readonly IList<string> _topics = new List<string>();
        private readonly string _groupId;
        private readonly string _clientId;
        private readonly IDictionary<string, ProcessorConfig> _processors;

        public string Name { get; set; }

        public string GroupId
        {
            get => _groupId ?? Name;
            init => _groupId = value;
        }
        public bool Disabled { get; init; }

        public string Topic { get; init; }

        public IList<string> Topics
        {
            get
            {
                if (!string.IsNullOrWhiteSpace(Topic) && !_topics.Contains(Topic))
                {
                    _topics.Add(Topic);
                }
                return _topics;
            }
            init => _topics = value?.ToList() ?? new List<string>();
        }
        public int MaxTasks { get; init; }
        public bool Paused { get; init; }
        public string Plugin { get; init; }
        public SinkConfig Sink { get; init; }
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
}