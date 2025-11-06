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
    public ZookeeperConfig Zookeeper { get; set; } = new();
    public KafkaConfig Broker { get; set; } = new();
    public SchemaRegistryConfig SchemaRegistry { get; set; } = new();
    public MongoDbConfig Mongo { get; set; } = new();
    public KafkaConnectConfig Worker { get; set; } = new();
}

public class NetworkConfig
{
    public string Name { get; set; } = "local";
}

public abstract class ContainerConfig
{
    public string Image { get; protected init; } = string.Empty;
    public string Name { get; protected init; } = string.Empty;
    public string Hostname { get; protected init; } = string.Empty;
    public List<string> Ports { get; set; } = [];
    public Dictionary<string, string> Environment { get; set; } = [];
    public List<string> NetworkAliases { get; set; } = [];
}

public class ZookeeperConfig : ContainerConfig
{
    public ZookeeperConfig()
    {
        Image = "confluentinc/cp-zookeeper:7.6.0";
        Name = "zookeeper";
        Hostname = "zookeeper";
    }
}

public class KafkaConfig : ContainerConfig
{
    public TestProducerConfig Producer { get; set; } = new();
    public TestConsumerConfig Consumer { get; set; } = new();

    public KafkaConfig()
    {
        Image = "confluentinc/cp-server:7.6.0";
        Name = "broker";
        Hostname = "broker";
    }
}

public class SchemaRegistryConfig : ContainerConfig
{
    public SchemaRegistryConfig()
    {
        Image = "confluentinc/cp-schema-registry:7.6.0";
        Name = "schema";
        Hostname = "schema";
    }
}

public class MongoDbConfig : ContainerConfig
{
    public string DatabaseName { get; set; } = "kafka_connect_test";
    public MongoDbConfig()
    {
        Image = "mongo:7.0";
        Name = "mongodb";
        Hostname = "mongodb";
    }
}

public class KafkaConnectConfig : ContainerConfig
{
    public string DockerfilePath { get; set; } = string.Empty;
    public string ConfigurationsPath { get; set; } = string.Empty;
    public bool WaitForHealthCheck { get; set; }
    public string HealthCheckEndpoint { get; set; } = string.Empty;
    public int StartupTimeoutSeconds { get; set; }
    public bool Enabled { get; set; } = true;
    public bool CleanUpImage { get; set; } = true;
    public List<string> Command { get; set; } = [];
    public Dictionary<string, string> BindMounts { get; set; } = new();

    public KafkaConnectConfig()
    {
        Name = "worker";
        Hostname = "worker";
        NetworkAliases = new List<string> { "worker" };
    }
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