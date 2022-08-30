namespace Kafka.Connect.FunctionalTests.Targets.Mongodb;

public class MongodbConfig
{
    public bool Disabled { get; set; }
    public string ConnectionString { get; set; } = "mongodb://localhost:27017";
    public string Database { get; set; } = "connectTests";
}