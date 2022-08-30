using Kafka.Connect.FunctionalTests.Targets.Mongodb;
using Microsoft.Extensions.Configuration;

namespace Kafka.Connect.FunctionalTests;

public class InitConfig
{
    public string RootFolder { get; set; } = "/tests";
    public string ConfigFile { get; set; } = "config.json";
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string SchemaRegistryUrl { get; set; } = "localhost:8081";

    public MongodbConfig Mongodb { get; set; }

    public static InitConfig Get()
    {
        var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
        return config.GetSection("connect").Get<InitConfig>();
    }
}