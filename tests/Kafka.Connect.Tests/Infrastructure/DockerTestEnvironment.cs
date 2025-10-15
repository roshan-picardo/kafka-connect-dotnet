using MongoDB.Driver;
using Confluent.Kafka;
using Testcontainers.MongoDb;
using Testcontainers.Kafka;
using Microsoft.Extensions.Logging;
using Xunit;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class DockerTestEnvironment : IAsyncLifetime
{
    private readonly ILogger<DockerTestEnvironment> _logger;
    
    public MongoDbContainer MongoDbContainer { get; private set; } = null!;
    public KafkaContainer KafkaContainer { get; private set; } = null!;
    
    public IMongoDatabase MongoDatabase { get; private set; } = null!;
    public IProducer<string, string> KafkaProducer { get; private set; } = null!;
    public IConsumer<string, string> KafkaConsumer { get; private set; } = null!;
    
    public string MongoConnectionString => MongoDbContainer.GetConnectionString();
    public string KafkaBootstrapServers => KafkaContainer.GetBootstrapAddress();

    public DockerTestEnvironment()
    {
        using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<DockerTestEnvironment>();
    }

    public async Task InitializeAsync()
    {
        _logger.LogInformation("Starting test containers...");

        // Start MongoDB container
        MongoDbContainer = new MongoDbBuilder()
            .WithImage("mongo:7.0")
            .WithUsername("admin")
            .WithPassword("password")
            .WithPortBinding(27017, true)
            .Build();

        await MongoDbContainer.StartAsync();
        _logger.LogInformation("MongoDB container started at: {ConnectionString}", MongoConnectionString);

        // Start Kafka container
        KafkaContainer = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.4.0")
            .WithPortBinding(9092, true)
            .Build();

        await KafkaContainer.StartAsync();
        _logger.LogInformation("Kafka container started at: {BootstrapServers}", KafkaBootstrapServers);

        // Initialize MongoDB client and database
        var mongoClient = new MongoClient(MongoConnectionString);
        MongoDatabase = mongoClient.GetDatabase("kafka_connect_test");
        
        // Create test collections
        await MongoDatabase.CreateCollectionAsync("mongo_sink_test");
        await MongoDatabase.CreateCollectionAsync("user_events");
        await MongoDatabase.CreateCollectionAsync("order_events");
        
        _logger.LogInformation("MongoDB collections created successfully");

        // Initialize Kafka producer
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = KafkaBootstrapServers,
            ClientId = "test-producer"
        };
        KafkaProducer = new ProducerBuilder<string, string>(producerConfig).Build();

        // Initialize Kafka consumer
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = KafkaBootstrapServers,
            GroupId = "test-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        KafkaConsumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        _logger.LogInformation("Test environment initialized successfully");
    }

    public async Task DisposeAsync()
    {
        _logger.LogInformation("Disposing test environment...");

        KafkaProducer?.Dispose();
        KafkaConsumer?.Dispose();

        if (KafkaContainer != null)
        {
            await KafkaContainer.DisposeAsync();
        }

        if (MongoDbContainer != null)
        {
            await MongoDbContainer.DisposeAsync();
        }

        _logger.LogInformation("Test environment disposed");
    }
}