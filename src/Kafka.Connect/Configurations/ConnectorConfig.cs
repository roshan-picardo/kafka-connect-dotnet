using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace Kafka.Connect.Configurations
{
    public class ConnectorConfig 
    {
        private readonly IList<string> _topics = new List<string>();
        private readonly string _groupId;
        private readonly string _clientId;

        public string Name { get; init; }

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
            init => _topics = value ?? new List<string>();
        }
        public int MaxTasks { get; init; }
        public bool Paused { get; init; }
        public string Plugin { get; init; }
        public SinkConfig Sink { get; init; }
        public ErrorsConfig Errors { get; init; }
        public RetryConfig Retries { get; init; }
        public EofConfig EofSignal { get; init; }
        public BatchConfig Batch { get; init; }
        public ConverterConfig Deserializers { get; set; }
        public IEnumerable<ProcessorConfig> Processors { get; set; }

        public string ClientId
        {
            get => _clientId ?? Name;
            init => _clientId = value;
        }
    }
}