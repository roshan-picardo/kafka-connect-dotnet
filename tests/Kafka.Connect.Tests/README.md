# Kafka Connect .NET End-to-End Testing Framework

This comprehensive testing framework provides end-to-end integration tests for all Kafka Connect .NET plugins, including MongoDB, PostgreSQL, MySQL, MariaDB, and SQL Server connectors.

## Overview

The testing framework includes:

- **Docker-based Test Environment**: Automated setup and teardown of all required services
- **Database Initialization**: C# helpers for setting up test data in all supported databases
- **Kafka Integration**: Producers and consumers for testing source and sink connectors
- **Comprehensive Test Coverage**: Tests for CRUD operations, CDC, batch processing, and error scenarios
- **Plugin-Specific Tests**: Dedicated test suites for each database connector

## Architecture

```
tests/Kafka.Connect.Tests/
├── Infrastructure/           # Core testing infrastructure
│   ├── BaseIntegrationTest.cs       # Base class for all integration tests
│   ├── DatabaseHelper.cs            # Database setup and utilities
│   ├── DockerTestEnvironment.cs     # Docker container management
│   ├── KafkaTestConsumer.cs         # Kafka consumer for testing
│   └── KafkaTestProducer.cs         # Kafka producer for testing
├── Plugins/                 # Plugin-specific test suites
│   ├── MongoDbPluginTests.cs        # MongoDB connector tests
│   ├── PostgreSqlPluginTests.cs     # PostgreSQL connector tests
│   ├── MariaDbPluginTests.cs        # MariaDB connector tests
│   ├── MySqlPluginTests.cs          # MySQL connector tests
│   └── SqlServerPluginTests.cs      # SQL Server connector tests
├── Configurations/          # Test configurations
│   └── appsettings.Testing.json     # Kafka Connect test configuration
├── docker-compose.test.yml  # Docker services for testing
├── Dockerfile.kafka-connect # Kafka Connect container definition
└── appsettings.json         # Test framework configuration
```

## Prerequisites

- Docker and Docker Compose
- .NET 8.0 SDK
- At least 8GB RAM (for running all database containers)
- Available ports: 9092 (Kafka), 27017 (MongoDB), 5432 (PostgreSQL), 1433 (SQL Server), 3306 (MySQL), 3307 (MariaDB), 6000 (Kafka Connect)

## Getting Started

### 1. Build the Solution

```bash
cd /path/to/kafka-connect-dotnet
dotnet build src/Kafka.sln
```

### 2. Run Tests

#### Using the Test Runner Script (Recommended)
```bash
cd tests/Kafka.Connect.Tests

# Run all tests
./run-tests.sh

# Run specific plugin tests
./run-tests.sh --mongodb --verbose
./run-tests.sh --postgresql
./run-tests.sh --mariadb
./run-tests.sh --mysql
./run-tests.sh --sqlserver

# Run with custom filter
./run-tests.sh --filter "SinkConnector"
```

#### Using dotnet test directly
```bash
cd tests/Kafka.Connect.Tests
dotnet test
```

#### Run Specific Plugin Tests
```bash
# MongoDB tests only
dotnet test --filter "FullyQualifiedName~MongoDbPluginTests"

# PostgreSQL tests only
dotnet test --filter "FullyQualifiedName~PostgreSqlPluginTests"

# MariaDB tests only
dotnet test --filter "FullyQualifiedName~MariaDbPluginTests"
```

#### Run with Detailed Logging
```bash
dotnet test --logger "console;verbosity=detailed"
```

### 3. Manual Docker Environment

You can also start the test environment manually for debugging:

```bash
cd tests/Kafka.Connect.Tests
docker-compose -f docker-compose.test.yml up -d

# When done, cleanup (preserves base images)
docker-compose -f docker-compose.test.yml down -v --remove-orphans
```

## Test Types

### Sink Connector Tests

These tests verify that data flows from Kafka topics to target databases:

1. **Insert Operations**: Verify new records are inserted correctly
2. **Update Operations**: Test CDC update messages are processed
3. **Delete Operations**: Ensure delete messages are handled appropriately
4. **Batch Processing**: Test high-volume message processing
5. **Error Handling**: Verify connector behavior with malformed data

Example:
```csharp
[Fact]
public async Task SinkConnector_Should_InsertRecordsToDatabase()
{
    // Arrange
    var testCustomers = new[] { /* test data */ };
    
    // Act - Produce messages to Kafka
    foreach (var customer in testCustomers)
    {
        await KafkaProducer.ProduceTestCustomerInsertAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
    }
    
    // Assert - Verify records in database
    var recordCount = await DatabaseHelper.GetCustomerCountAsync(connectionString, tableName);
    recordCount.Should().BeGreaterOrEqualTo(testCustomers.Length);
}
```

### Source Connector Tests

These tests verify that database changes are captured and published to Kafka:

1. **Change Detection**: Verify INSERT, UPDATE, DELETE operations are captured
2. **Message Format**: Ensure CDC messages have correct structure
3. **Snapshot Mode**: Test initial data loading capabilities
4. **Real-time Changes**: Verify ongoing change capture

Example:
```csharp
[Fact]
public async Task SourceConnector_Should_PublishRecordsFromDatabase()
{
    // Arrange
    using var consumer = await CreateKafkaConsumerAsync("test-group");
    
    // Act - Insert records into database
    await DatabaseHelper.InsertCustomerAsync(connectionString, id, name, email, age);
    
    // Assert - Verify messages published to Kafka
    var messages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(2));
    messages.Should().HaveCount(1);
}
```

## Configuration

### Test Configuration (appsettings.json)

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "SchemaRegistryUrl": "http://localhost:28081"
  },
  "MongoDB": {
    "ConnectionString": "mongodb://admin:admin123@localhost:27017/kafka_connect_test?authSource=admin",
    "Database": "kafka_connect_test"
  },
  "PostgreSQL": {
    "ConnectionString": "Host=localhost;Port=5432;Database=kafka_connect_test;Username=postgres;Password=postgres123"
  }
  // ... other database configurations
}
```

### Kafka Connect Configuration (Configurations/appsettings.Testing.json)

This file contains the complete Kafka Connect configuration with all connectors pre-configured for testing:

- Source connectors for each database
- Sink connectors for each database
- Optimized settings for testing (smaller batches, faster polling)
- Test-specific topics and connection strings

## Database Helpers

The `DatabaseHelper` class provides C# methods for database operations:

```csharp
// MongoDB
await DatabaseHelper.MongoDB.InitializeAsync(connectionString, databaseName);
await DatabaseHelper.MongoDB.InsertCustomerAsync(connectionString, databaseName, id, name, email, age);

// PostgreSQL
await DatabaseHelper.PostgreSQL.InitializeAsync(connectionString);
await DatabaseHelper.PostgreSQL.UpdateCustomerAsync(connectionString, id, name, email, age);

// MySQL/MariaDB
await DatabaseHelper.MySQL.InitializeAsync(connectionString);
await DatabaseHelper.MySQL.DeleteCustomerAsync(connectionString, id);

// SQL Server
await DatabaseHelper.SqlServer.InitializeAsync(connectionString);
var count = await DatabaseHelper.SqlServer.GetCustomerCountAsync(connectionString, tableName);
```

## Kafka Test Utilities

### KafkaTestProducer

```csharp
// Simple message production
await KafkaProducer.ProduceAsync(topic, key, value);

// CDC-style messages
await KafkaProducer.ProduceTestCustomerInsertAsync(topic, id, name, email, age);
await KafkaProducer.ProduceTestCustomerUpdateAsync(topic, id, oldName, oldEmail, oldAge, newName, newEmail, newAge);
await KafkaProducer.ProduceTestCustomerDeleteAsync(topic, id, name, email, age);

// Batch production
await KafkaProducer.ProduceBatchAsync(topic, messages);
```

### KafkaTestConsumer

```csharp
// Consume specific number of messages
var messages = await consumer.ConsumeMessagesAsync(topic, expectedCount, timeout);

// Consume all available messages
var messages = await consumer.ConsumeAllAvailableMessagesAsync(topic, timeout);

// Deserialize message content
var customerData = message.DeserializeValue<CustomerModel>();
```

## Docker Environment Management

The `DockerTestEnvironment` class manages the complete test infrastructure:

```csharp
// Start all services
await DockerEnvironment.StartAsync();

// Check service health
var isHealthy = await DockerEnvironment.IsServiceHealthyAsync("kafka-connect");

// Restart specific service
await DockerEnvironment.RestartServiceAsync("postgres");

// Get service logs
var logs = await DockerEnvironment.GetServiceLogsAsync("kafka-connect", 100);

// Cleanup (preserves base images, only removes custom Kafka Connect image)
await DockerEnvironment.DisposeAsync();
```

### Image Management

The test framework is optimized for repeated test runs by preserving Docker base images:

- **Preserved Images**: All base images (Kafka, databases, etc.) are kept between test runs
- **Removed Images**: Only the custom Kafka Connect image (built from Dockerfile) is removed during cleanup
- **Benefits**: Faster subsequent test runs, reduced bandwidth usage, improved developer experience

This approach ensures that:
1. Base images like `confluentinc/cp-server`, `postgres:16.1`, `mongo:7.0` etc. are downloaded only once
2. Only the custom-built Kafka Connect image is rebuilt when code changes
3. Test environment startup is significantly faster after the first run

## Test Data Strategy

Each plugin test uses unique ID ranges to avoid conflicts:

- MongoDB: 100-999
- PostgreSQL: 1100-1999
- MariaDB: 2100-2999
- MySQL: 3100-3999
- SQL Server: 4100-4999

## Troubleshooting

### Common Issues

1. **Port Conflicts**: Ensure required ports are available
2. **Memory Issues**: Increase Docker memory allocation
3. **Timeout Issues**: Increase test timeouts for slower systems
4. **Container Startup**: Check Docker logs for service startup issues

### Debugging

1. **Enable Detailed Logging**:
   ```bash
   dotnet test --logger "console;verbosity=detailed"
   ```

2. **Check Kafka Connect Logs**:
   ```bash
   docker-compose -f docker-compose.test.yml logs kafka-connect
   ```

3. **Inspect Database State**:
   ```bash
   # PostgreSQL
   docker exec -it test-postgres psql -U postgres -d kafka_connect_test -c "SELECT * FROM test_customers;"
   
   # MongoDB
   docker exec -it test-mongodb mongosh kafka_connect_test --eval "db.test_customers.find()"
   ```

4. **Monitor Kafka Topics**:
   ```bash
   docker exec -it test-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic test-source-topic --from-beginning
   ```

## Performance Considerations

- Tests run sequentially to avoid resource conflicts
- Each test class uses isolated data ranges
- Docker containers are shared across tests for efficiency
- Database initialization is optimized for test speed

## Contributing

When adding new tests:

1. Extend `BaseIntegrationTest` for infrastructure access
2. Use unique ID ranges for test data
3. Include comprehensive assertions
4. Add appropriate logging
5. Handle cleanup in test disposal
6. Follow the existing naming conventions

## CI/CD Integration

The test framework is designed for CI/CD environments:

- All dependencies are containerized
- Tests are deterministic and isolated
- Comprehensive logging for debugging
- Configurable timeouts for different environments
- Health checks ensure services are ready before testing