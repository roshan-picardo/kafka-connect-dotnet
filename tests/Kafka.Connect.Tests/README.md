# Kafka Connect Integration Tests

This directory contains comprehensive integration tests for the Kafka Connect .NET application, including both infrastructure tests and full end-to-end tests with the complete Kafka Connect application running in Docker.

## Overview

There are two types of integration tests available:

1. **Infrastructure Tests** (`BaseIntegrationTest`) - Test database operations and Kafka messaging using Testcontainers
2. **Full Integration Tests** (`DockerizedIntegrationTest`) - Test the complete Kafka Connect application with all components running in Docker

## Quick Start

### Prerequisites

- Docker Desktop installed and running
- .NET 8.0 SDK
- Docker Compose

### Running Full Integration Tests (Recommended)

```bash
# Navigate to the test directory
cd tests/Kafka.Connect.Tests

# Run the complete integration test suite
./run-integration-tests.sh

# Or with verbose output
./run-integration-tests.sh -v

# Or start services only for manual testing
./run-integration-tests.sh -s
```

### Running Infrastructure Tests Only

```bash
# Run infrastructure tests (no Kafka Connect application)
dotnet test Kafka.Connect.Tests.csproj --configfile nuget.debug.config
```

## Docker Environment

### Architecture

The Docker environment includes:

- **Zookeeper** - Kafka coordination service
- **Kafka** - Message broker (confluentinc/cp-kafka:7.4.0)
- **MySQL** - Database (mysql:8.0)
- **PostgreSQL** - Database (postgres:15)
- **MongoDB** - Database (mongo:7.0)
- **Kafka Connect** - The main application (built from source in debug mode)
- **Test Runner** - Container that executes the tests

### Services and Ports

| Service | Internal Port | External Port | Health Check |
|---------|---------------|---------------|--------------|
| Kafka | 29092 | 9092 | Broker API versions |
| MySQL | 3306 | 3306 | mysqladmin ping |
| PostgreSQL | 5432 | 5432 | pg_isready |
| MongoDB | 27017 | 27017 | mongosh ping |
| Kafka Connect | 8083 | 8083 | HTTP /health |

### Configuration Files

- [`docker-compose.test.yml`](docker-compose.test.yml) - Complete Docker environment
- [`appsettings.testing.json`](appsettings.testing.json) - Kafka Connect configuration for testing
- [`run-integration-tests.sh`](run-integration-tests.sh) - Test runner script

## Test Types

### 1. Infrastructure Tests (`*PluginTests.cs`)

These tests use `BaseIntegrationTest` and focus on:
- Database connectivity and operations
- Kafka producer/consumer functionality
- Data seeding and cleanup
- Basic plugin infrastructure

**Example:**
```csharp
public class MySqlPluginTests : BaseIntegrationTest
{
    [Fact]
    public async Task Should_Connect_To_MySQL_Successfully()
    {
        // Test database connection
    }
}
```

### 2. Full Integration Tests (`Dockerized*PluginTests.cs`)

These tests use `DockerizedIntegrationTest` and test:
- Complete Kafka Connect application
- Connector lifecycle management
- Real data streaming from databases to Kafka
- REST API functionality
- Plugin loading and configuration

**Example:**
```csharp
public class DockerizedMySqlPluginTests : DockerizedIntegrationTest
{
    [Fact]
    public async Task Should_Stream_MySQL_Changes_To_Kafka()
    {
        // Test complete data streaming pipeline
    }
}
```

## Test Runner Script Usage

The [`run-integration-tests.sh`](run-integration-tests.sh) script provides comprehensive test execution:

```bash
# Show help
./run-integration-tests.sh --help

# Run all tests with default settings
./run-integration-tests.sh

# Verbose output with image rebuild
./run-integration-tests.sh -v -r

# Start services only for manual testing
./run-integration-tests.sh -s

# Run specific test pattern
./run-integration-tests.sh -t "MySqlPluginTests"

# Keep containers running after tests
./run-integration-tests.sh --no-cleanup

# Custom wait time for services
./run-integration-tests.sh -w 120
```

### Script Options

| Option | Description |
|--------|-------------|
| `-h, --help` | Show help message |
| `-v, --verbose` | Enable verbose output |
| `-r, --rebuild` | Force rebuild of Docker images |
| `-s, --services-only` | Start services only (don't run tests) |
| `-n, --no-cleanup` | Don't cleanup containers on exit |
| `-w, --wait SECONDS` | Wait time for services to be ready (default: 60) |
| `-t, --test PATTERN` | Run specific test pattern |
| `-c, --config FILE` | Use custom docker-compose file |

## Manual Testing

### Start Services for Manual Testing

```bash
# Start all services
./run-integration-tests.sh -s

# Services will be available at:
# - Kafka Connect API: http://localhost:8083
# - MySQL: localhost:3306
# - PostgreSQL: localhost:5432
# - MongoDB: localhost:27017
# - Kafka: localhost:9092
```

### Kafka Connect REST API

```bash
# List connector plugins
curl http://localhost:8083/connector-plugins

# Create a MySQL source connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-source",
    "config": {
      "connector.class": "Kafka.Connect.MySql.MySqlSourceConnector",
      "tasks.max": "1",
      "connection.url": "Server=localhost;Port=3306;Database=testdb;Uid=testuser;Pwd=testpass;",
      "table.whitelist": "integration_users",
      "mode": "incrementing",
      "incrementing.column.name": "id",
      "topic.prefix": "mysql-"
    }
  }'

# Check connector status
curl http://localhost:8083/connectors/mysql-source/status

# List all connectors
curl http://localhost:8083/connectors

# Delete connector
curl -X DELETE http://localhost:8083/connectors/mysql-source
```

### Database Access

```bash
# MySQL
mysql -h localhost -P 3306 -u testuser -ptestpass testdb

# PostgreSQL
psql -h localhost -p 5432 -U testuser -d testdb

# MongoDB
mongosh "mongodb://testuser:testpass@localhost:27017/testdb?authSource=admin"
```

## Troubleshooting

### Common Issues

1. **Docker not running**
   ```bash
   # Check Docker status
   docker info
   
   # Start Docker Desktop
   ```

2. **Port conflicts**
   ```bash
   # Check what's using ports
   lsof -i :9092  # Kafka
   lsof -i :8083  # Kafka Connect
   lsof -i :3306  # MySQL
   ```

3. **Services not ready**
   ```bash
   # Check service logs
   docker-compose -f docker-compose.test.yml logs kafka-connect
   docker-compose -f docker-compose.test.yml logs kafka
   ```

4. **Build failures**
   ```bash
   # Force rebuild
   ./run-integration-tests.sh -r
   
   # Clean Docker cache
   docker system prune -a
   ```

### Debugging

1. **View service status**
   ```bash
   docker-compose -f docker-compose.test.yml ps
   ```

2. **Check service health**
   ```bash
   # Kafka Connect
   curl http://localhost:8083/health
   
   # Kafka
   docker-compose -f docker-compose.test.yml exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

3. **View logs**
   ```bash
   # All services
   docker-compose -f docker-compose.test.yml logs -f
   
   # Specific service
   docker-compose -f docker-compose.test.yml logs -f kafka-connect
   ```

4. **Access containers**
   ```bash
   # Kafka Connect container
   docker-compose -f docker-compose.test.yml exec kafka-connect sh
   
   # MySQL container
   docker-compose -f docker-compose.test.yml exec mysql mysql -u testuser -ptestpass testdb
   ```

## Test Results

Test results are saved to:
- `TestResults/TestResults.trx` - MSTest results
- `TestResults/` - Coverage reports and artifacts
- Container logs in Docker volumes

## Cleanup

```bash
# Stop and remove all containers
docker-compose -f docker-compose.test.yml down -v

# Remove all test-related containers and volumes
docker-compose -f docker-compose.test.yml down -v --remove-orphans

# Clean up Docker system
docker system prune -f
```

## Development

### Adding New Tests

1. **Infrastructure Test** - Extend `BaseIntegrationTest`
2. **Full Integration Test** - Extend `DockerizedIntegrationTest`

### Modifying Docker Environment

1. Update [`docker-compose.test.yml`](docker-compose.test.yml)
2. Update [`appsettings.testing.json`](appsettings.testing.json)
3. Test changes with `./run-integration-tests.sh -r`

### Performance Considerations

- First run downloads Docker images (~2-5 minutes)
- Subsequent runs use cached images (~30-60 seconds startup)
- Use `--no-cleanup` for faster iteration during development
- Use `-s` to start services once for multiple manual tests