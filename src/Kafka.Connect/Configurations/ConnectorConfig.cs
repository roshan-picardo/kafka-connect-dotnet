using System.Collections.Generic;
using Kafka.Connect.Providers;
// ReSharper disable UnusedAutoPropertyAccessor.Global

namespace Kafka.Connect.Configurations;

public class ConnectorConfig
{
    private readonly string _groupId;
    private readonly string _clientId;
    private readonly LogConfig _log;
    private readonly Dictionary<string, TopicConfig> _topics = new();
    
    public ConverterConfig Converters { get; init; }
    
    // ReSharper disable once InconsistentNaming ::Allowing to use an array for topics
    // ReSharper disable once UnusedAutoPropertyAccessor.Local
    private string[] topics { get; init; }

    public Dictionary<string, TopicConfig> Topics
    {
        get
        {
            if (topics != null && topics.Length != 0)
            {
                _topics.Clear();
                foreach (var topic in topics)
                {
                    _topics.Add(topic, new TopicConfig());
                }
            }

            return _topics;
        }
        init => _topics = value;
    }

    public string Name { get; set; }

    public string GroupId
    {
        get => _groupId ?? Name;
        init => _groupId = value;
    }
    public bool Disabled { get; init; }

    public int MaxTasks { get; init; }
    public bool Paused { get; init; }
    public PluginConfig Plugin { get; init; }

    public LogConfig Log
    {
        get => _log ?? new LogConfig { Provider = typeof(DefaultLogRecord).FullName };
        init => _log = value;
    }

    public BatchConfigOld Batches { get; init; }
        
    public IDictionary<int, ProcessorConfig> Processors { get; init; }

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

public enum TopicType
{
    None = 0,
    Command,
    Config
}