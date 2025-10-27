# Kafka Connect Integration Tests

This project contains integration tests for the Kafka Connect .NET implementation using **Testcontainers** for lightweight, isolated testing environments.

## Architecture Overview

The integration tests use **Testcontainers** to provide:
- **Kafka Container**: For message streaming and topic management
- **MongoDB Container**: For sink connector testing
- **Automatic Lifecycle Management**: Containers are started/stopped automatically per test collection

## Key Components

### 1. TestFixture
- **File**: [`Infrastructure/TestFixture.cs`](Infrastructure/TestFixture.cs)
- **Purpose**: xUnit collection fixture that manages Kafka and MongoDB containers
- **Features**:
  - Implements `IAsyncLifetime` for proper container lifecycle
  - Provides Kafka producer/consumer clients
  - Provides MongoDB database access
  - Automatic topic creation and message production utilities
  - Clean resource disposal with Ryuk cleanup

### 2. Integration Test Collection
- **File**: [`Plugins/MongoDbTests.cs`](Plugins/MongoDbTests.cs)
- **Purpose**: Comprehensive integration tests using shared container infrastructure
- **Test Coverage**:
  - MongoDB connectivity and operations
  - Kafka topic creation and message production
  - End-to-end Kafka → MongoDB data flow simulation
  - Container accessibility verification

### 3. xUnit Configuration
- **File**: [`xunit.runner.json`](xunit.runner.json)
- **Purpose**: Optimizes test execution and logging
- **Features**:
  - Single-threaded execution to prevent logging conflicts
  - Disabled diagnostic messages for cleaner output
  - Configured for integration test scenarios

## Test Execution

### Running Tests
```bash
# Run all integration tests
cd tests/Kafka.Connect.Tests
dotnet test

# Run with minimal logging
dotnet test --logger "console;verbosity=minimal"

# Run specific test class
dotnet test --filter "MongoDbTests"
```

### Test Collection Architecture
```
TestCollection (ICollectionFixture<TestFixture>)
├── TestFixture (IAsyncLifetime)
│   ├── KafkaContainer (confluentinc/cp-kafka:7.6.0)
│   ├── MongoDbContainer (mongo:7.0)
│   ├── Kafka Clients (Producer, Consumer, AdminClient)
│   └── MongoDB Database (kafka_connect_test)
└── MongoDbTests
    ├── MongoDB_ShouldBeAccessible
    ├── MongoDB_ShouldAllowInsertAndQuery
    ├── Kafka_ShouldBeAccessible
    ├── Kafka_ShouldCreateTopicAndProduceMessage
    └── KafkaAndMongoDB_ShouldWorkTogether
```

## Container Configuration

### Kafka Container
- **Image**: `confluentinc/cp-kafka:7.6.0`
- **Features**: Auto-create topics, single partition, single replica
- **Ports**: Fixed ports - 9092 (broker), 9093 (internal), 2181 (zookeeper)
- **Environment**: Configured for single-node testing

### MongoDB Container
- **Image**: `mongo:7.0`
- **Authentication**: admin/admin
- **Database**: `kafka_connect_test`
- **Collections**: Pre-created test collections for various scenarios
- **Port**: Fixed port - 27017 (default MongoDB port)

## Benefits of Testcontainers Approach

1. **Isolation**: Each test run gets fresh containers
2. **Speed**: Faster than Docker Compose for test scenarios
3. **Reliability**: Automatic cleanup prevents resource leaks
4. **Portability**: Works across different environments without external dependencies
5. **Debugging**: Better integration with IDE debugging tools
6. **CI/CD Friendly**: No external Docker Compose files required

## Configuration Files

### Package Dependencies
- **Testcontainers**: Core container management
- **Testcontainers.Kafka**: Kafka-specific container support
- **Testcontainers.MongoDb**: MongoDB-specific container support
- **Confluent.Kafka**: Kafka client library
- **MongoDB.Driver**: MongoDB client library

### Test Settings
- **Target Framework**: .NET 8.0
- **Test Framework**: xUnit with collection fixtures
- **Logging**: Minimal console output with Testcontainers built-in logging
- **Parallelization**: Disabled for stable container management

## Troubleshooting

### Common Issues
1. **Docker not running**: Ensure Docker Desktop is running
2. **Port conflicts**: Testcontainers handles port assignment automatically
3. **Container cleanup**: Ryuk container handles automatic cleanup
4. **Logging duplicates**: Resolved via xunit.runner.json configuration

### Performance Tips
- Tests reuse containers within the same collection
- Container images are cached after first pull
- Ryuk ensures no orphaned containers remain

## Migration Notes

This implementation replaces the previous Docker Compose-based approach with:
- ✅ **Removed**: DockerTestEnvironment.cs (obsolete)
- ✅ **Added**: TestFixture.cs (new approach)
- ✅ **Improved**: Single-threaded execution prevents logging conflicts
- ✅ **Enhanced**: Better resource management and cleanup