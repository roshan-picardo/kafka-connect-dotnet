namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class TestConfiguration
{
    public bool DetailedLog { get; set; } = true;
    public bool RawJsonLog { get; set; } = false;
    public bool SkipInfrastructure { get; set; } = false;
    public TestContainersConfig TestContainers { get; set; } = new();
    public ShakedownConfig Shakedown { get; set; } = new();
}

public class TestContainersConfig
{
    public NetworkConfig Network { get; set; } = new();
    public Dictionary<string, ContainerConfig> Containers { get; set; } = new();
    public TestProducerConfig Producer { get; set; } = new();
    public TestConsumerConfig Consumer { get; set; } = new();
}

public class NetworkConfig
{
    public string Name { get; set; } = "local";
}

public class ContainerConfig
{
    public string Image { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Hostname { get; set; } = string.Empty;
    public List<string> Ports { get; set; } = [];
    public Dictionary<string, string> Environment { get; set; } = [];
    public List<string> NetworkAliases { get; set; } = [];
    
    // Optional properties for special containers
    public string? DockerfilePath { get; set; }
    public string? ConfigurationsPath { get; set; }
    public Dictionary<string, string> BindMounts { get; set; } = [];
    public List<string> Command { get; set; } = [];
    public bool CleanUpImage { get; set; } = true;
    public bool WaitForHealthCheck { get; set; }
    public string HealthCheckEndpoint { get; set; } = string.Empty;
    public int StartupTimeoutSeconds { get; set; }
    public bool Enabled { get; set; } = true;
}

public class TestProducerConfig
{
    public string ClientId { get; set; } = "testcontainers-producer";
}

public class TestConsumerConfig
{
    public string GroupId { get; set; } = "testcontainers-consumer-group";
    public string AutoOffsetReset { get; set; } = "Earliest";
    public bool EnableAutoCommit { get; set; } = false;
}

public class ShakedownConfig
{
    public string Kafka { get; set; } = string.Empty;
    public string SchemaRegistry { get; set; } = string.Empty;
    public string Mongo { get; set; } = string.Empty;
    public string Postgres { get; set; } = string.Empty;
    public string Worker { get; set; } = string.Empty;
}

public class TestPriorityAttribute : Attribute
{
    public int Priority { get; }
        
    public TestPriorityAttribute(int priority)
    {
        Priority = priority;
    }
}