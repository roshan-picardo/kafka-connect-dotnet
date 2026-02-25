# Kafka Connect .NET

[![.NET](https://github.com/roshan-picardo/kafka-connect-dotnet/actions/workflows/dotnet.yml/badge.svg)](https://github.com/roshan-picardo/kafka-connect-dotnet/actions/workflows/dotnet.yml) [![Tests](https://img.shields.io/badge/tests-27%20passed-success)](https://github.com/roshan-picardo/kafka-connect-dotnet)

A .NET implementation of Kafka Connect framework for building scalable and reliable data pipelines between Apache Kafka and external systems.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Supported Connectors](#supported-connectors)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Connector Examples](#connector-examples)
- [Processors](#processors)
- [Strategies](#strategies)
- [Building and Running](#building-and-running)
- [Docker Support](#docker-support)
- [Contributing](#contributing)
- [License](#license)

## Overview

Kafka Connect .NET is a framework for connecting Apache Kafka with external systems such as databases, key-value stores, and other data sources. It provides a scalable and fault-tolerant way to stream data between Kafka and these systems.

### Key Concepts

- **Connectors**: High-level abstraction that coordinates data streaming (Source or Sink)
- **Tasks**: Implementation that actually copies data to/from Kafka
- **Workers**: Processes that execute connectors and tasks
- **Leader**: Coordinates worker nodes and manages connector distribution
- **Converters**: Handle serialization/deserialization of data
- **Processors**: Transform data as it flows through the pipeline
- **Strategies**: Define how data operations (insert, update, delete, upsert) are executed

## Features

- ✅ **Multiple Database Support**: MongoDB, PostgreSQL, SQL Server, MySQL, MariaDB, Oracle, DynamoDB
- ✅ **Source & Sink Connectors**: Read from and write to external systems
- ✅ **Distributed Architecture**: Leader-worker model for scalability
- ✅ **Fault Tolerance**: Automatic retries, error handling, and failover
- ✅ **Data Transformation**: Built-in processors for field projection, renaming, and type conversion
- ✅ **Multiple Strategies**: Insert, Update, Delete, Upsert, and Read operations
- ✅ **Schema Support**: JSON, Avro, and JSON Schema converters
- ✅ **Health Monitoring**: Built-in health checks and monitoring
- ✅ **Plugin Architecture**: Extensible design for custom connectors
- ✅ **Change Data Capture**: Support for CDC patterns with changelog tables
- ✅ **Batch Processing**: Configurable batch sizes and parallelism

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Kafka Cluster                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Topic A    │  │   Topic B    │  │   Config     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                            ▲  │
                            │  ▼
┌─────────────────────────────────────────────────────────────┐
│                    Kafka Connect .NET                        │
│                                                               │
│  ┌──────────────┐                                            │
│  │    Leader    │  ← Coordinates workers and connectors      │
│  └──────────────┘                                            │
│         │                                                     │
│         ├─────────┬─────────┬─────────┐                     │
│         ▼         ▼         ▼         ▼                     │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐          │
│  │Worker 1 │ │Worker 2 │ │Worker 3 │ │Worker N │          │
│  │         │ │         │ │         │ │         │          │
│  │Connector│ │Connector│ │Connector│ │Connector│          │
│  │  Tasks  │ │  Tasks  │ │  Tasks  │ │  Tasks  │          │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘          │
└─────────────────────────────────────────────────────────────┘
       │              │              │              │
       ▼              ▼              ▼              ▼
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│ MongoDB  │   │PostgreSQL│   │SQL Server│   │  MySQL   │
└──────────┘   └──────────┘   └──────────┘   └──────────┘
```

## Supported Connectors

| Connector | Source | Sink | Strategies |
|-----------|--------|------|------------|
| **MongoDB** | ✅ (Change Streams) | ✅ | Insert, Update, Delete, Upsert, Read, StreamsRead |
| **PostgreSQL** | ✅ (CDC) | ✅ | Insert, Update, Delete, Upsert, Read |
| **SQL Server** | ✅ (CDC) | ✅ | Insert, Update, Delete, Upsert, Read |
| **MySQL** | ✅ (CDC) | ✅ | Insert, Update, Delete, Upsert, Read |
| **MariaDB** | ✅ (CDC) | ✅ | Insert, Update, Delete, Upsert, Read |
| **Oracle** | ✅ (CDC) | ✅ | Insert, Update, Delete, Upsert, Read |
| **DynamoDB** | ✅ (Streams) | ✅ | Insert, Update, Delete, Upsert, Read, StreamRead |
| **Replicator** | ✅ | ✅ | Topic-to-Topic replication |

## Getting Started

### Prerequisites

- .NET 8.0 or later
- Apache Kafka cluster
- Target database system (MongoDB, PostgreSQL, etc.)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/roshan-picardo/kafka-connect-dotnet.git
cd kafka-connect-dotnet
```

2. Build the solution:
```bash
dotnet build Kafka.sln
```

3. Run tests (optional):
```bash
dotnet test
```

## Configuration

Kafka Connect .NET uses JSON configuration files for both leader and worker nodes.

### Leader Configuration

The leader node coordinates workers and manages connector distribution. See [`docs/appsettings.leader.md`](docs/appsettings.leader.md) for detailed configuration.

**Key settings:**
- Kafka connection settings
- Internal topics for coordination
- Health check configuration
- Plugin locations

### Worker Configuration

Worker nodes execute connector tasks. See [`docs/appsettings.worker.md`](docs/appsettings.worker.md) for detailed configuration.

**Key settings:**
- Kafka connection settings
- Fault tolerance and retry policies
- Batch processing configuration
- Converter settings
- Connector configurations

### Basic Configuration Structure

```json
{
  "worker": {
    "bootstrapServers": "localhost:9092",
    "topics": {
      "connector-config-topic": { "purpose": "config" }
    },
    "converters": {
      "key": "Kafka.Connect.Converters.JsonConverter",
      "value": "Kafka.Connect.Converters.JsonConverter"
    },
    "plugins": {
      "location": "/path/to/plugins/",
      "initializers": {
        "default-mongodb": {
          "prefix": "mongodb",
          "assembly": "Kafka.Connect.MongoDb.dll",
          "class": "Kafka.Connect.MongoDb.DefaultPluginInitializer"
        }
      }
    },
    "connectors": []
  }
}
```

## Connector Examples

### MongoDB Sink Connector

Stream data from Kafka to MongoDB:

```json
{
  "worker": {
    "connectors": {
      "mongodb-sink-connector": {
        "groupId": "mongodb-sink-connector",
        "topics": ["user-events"],
        "tasks": 1,
        "plugin": {
          "type": "sink",
          "name": "mongodb",
          "handler": "Kafka.Connect.MongoDb.MongoPluginHandler",
          "strategy": {
            "name": "Kafka.Connect.MongoDb.Strategies.UpsertStrategy"
          },
          "properties": {
            "connectionUri": "mongodb://localhost:27017",
            "database": "mydb",
            "collection": "users",
            "filter": "{\"userId\": \"#userId#\"}"
          }
        }
      }
    }
  }
}
```

### PostgreSQL Source Connector

Stream changes from PostgreSQL to Kafka using CDC:

```json
{
  "worker": {
    "connectors": {
      "postgres-source-connector": {
        "groupId": "postgres-source-connector",
        "tasks": 1,
        "plugin": {
          "type": "source",
          "name": "postgres",
          "handler": "Kafka.Connect.Postgres.PostgresPluginHandler",
          "strategy": {
            "name": "Kafka.Connect.Postgres.Strategies.ReadStrategy"
          },
          "properties": {
            "host": "localhost",
            "port": 5432,
            "userId": "postgres",
            "password": "password",
            "database": "mydb",
            "changelog": {
              "schema": "public",
              "table": "audit_log"
            },
            "commands": {
              "read-users": {
                "topic": "user-changes",
                "table": "users",
                "schema": "public",
                "keys": ["userId"],
                "snapshot": {
                  "enabled": true,
                  "key": "userId"
                }
              }
            }
          }
        }
      }
    }
  }
}
```

### SQL Server Sink Connector

```json
{
  "worker": {
    "connectors": {
      "sqlserver-sink-connector": {
        "groupId": "sqlserver-sink-connector",
        "topics": ["orders"],
        "tasks": 1,
        "plugin": {
          "type": "sink",
          "name": "sqlserver",
          "handler": "Kafka.Connect.SqlServer.SqlServerPluginHandler",
          "strategy": {
            "name": "Kafka.Connect.SqlServer.Strategies.InsertStrategy"
          },
          "properties": {
            "server": "localhost",
            "port": 1433,
            "userId": "sa",
            "password": "YourPassword",
            "database": "OrdersDB",
            "schema": "dbo",
            "table": "Orders"
          }
        }
      }
    }
  }
}
```

### DynamoDB Connector

```json
{
  "worker": {
    "connectors": {
      "dynamodb-sink-connector": {
        "groupId": "dynamodb-sink-connector",
        "topics": ["events"],
        "tasks": 1,
        "plugin": {
          "type": "sink",
          "name": "dynamodb",
          "handler": "Kafka.Connect.DynamoDb.DynamoDbPluginHandler",
          "strategy": {
            "name": "Kafka.Connect.DynamoDb.Strategies.UpsertStrategy"
          },
          "properties": {
            "region": "us-east-1",
            "accessKeyId": "YOUR_ACCESS_KEY",
            "secretAccessKey": "YOUR_SECRET_KEY",
            "tableName": "Events",
            "filter": "userId={userId}"
          }
        }
      }
    }
  }
}
```

### MariaDB/MySQL Connectors

Both MariaDB and MySQL use similar configurations:

```json
{
  "worker": {
    "connectors": {
      "mariadb-sink-connector": {
        "groupId": "mariadb-sink-connector",
        "topics": ["products"],
        "tasks": 1,
        "plugin": {
          "type": "sink",
          "name": "mariadb",
          "handler": "Kafka.Connect.MariaDb.MariaDbPluginHandler",
          "strategy": {
            "name": "Kafka.Connect.MariaDb.Strategies.UpsertStrategy"
          },
          "properties": {
            "host": "localhost",
            "port": 3306,
            "userId": "root",
            "password": "password",
            "database": "inventory",
            "schema": "inventory",
            "table": "products",
            "lookup": "`productId`='#productId#'",
            "filter": "`productId`='#productId#'"
          }
        }
      }
    }
  }
}
```

## Processors

Processors transform data as it flows through the pipeline. They can be chained together for complex transformations.

### Available Processors

#### 1. WhitelistFieldProjector
Includes only specified fields in the output:

```json
{
  "processors": {
    "1": {
      "name": "Kafka.Connect.Processors.WhitelistFieldProjector",
      "settings": ["userId", "userName", "email", "timestamp"]
    }
  }
}
```

#### 2. BlacklistFieldProjector
Excludes specified fields from the output:

```json
{
  "processors": {
    "1": {
      "name": "Kafka.Connect.Processors.BlacklistFieldProjector",
      "settings": ["password", "ssn", "creditCard"]
    }
  }
}
```

#### 3. FieldRenamer
Renames fields in the data:

```json
{
  "processors": {
    "1": {
      "name": "Kafka.Connect.Processors.FieldRenamer",
      "settings": {
        "oldFieldName": "newFieldName",
        "user_id": "userId",
        "created_at": "timestamp"
      }
    }
  }
}
```

#### 4. DateTimeTypeOverrider
Converts date/time fields to specific formats:

```json
{
  "processors": {
    "1": {
      "name": "Kafka.Connect.Processors.DateTimeTypeOverrider",
      "settings": {
        "timestamp": "yyyy-MM-dd HH:mm:ss",
        "createdDate": "yyyy-MM-dd"
      }
    }
  }
}
```

#### 5. JsonTypeOverrider
Converts field types in JSON data:

```json
{
  "processors": {
    "1": {
      "name": "Kafka.Connect.Processors.JsonTypeOverrider",
      "settings": {
        "age": "int",
        "price": "decimal",
        "isActive": "bool"
      }
    }
  }
}
```

### Chaining Processors

Processors can be chained by using numbered keys:

```json
{
  "processors": {
    "1": {
      "name": "Kafka.Connect.Processors.BlacklistFieldProjector",
      "settings": ["password", "ssn"]
    },
    "2": {
      "name": "Kafka.Connect.Processors.FieldRenamer",
      "settings": {
        "user_id": "userId"
      }
    },
    "3": {
      "name": "Kafka.Connect.Processors.DateTimeTypeOverrider",
      "settings": {
        "timestamp": "yyyy-MM-dd HH:mm:ss"
      }
    }
  }
}
```

## Strategies

Strategies define how data operations are executed against the target system.

### Sink Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **InsertStrategy** | Inserts new records | When you only need to add new data |
| **UpdateStrategy** | Updates existing records | When you need to modify existing data |
| **DeleteStrategy** | Deletes records | When you need to remove data |
| **UpsertStrategy** | Insert or update (if exists) | Most common - handles both new and existing data |

### Source Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **ReadStrategy** | Reads data using changelog table | CDC pattern with audit table |
| **StreamsReadStrategy** | Reads from change streams | MongoDB change streams |
| **StreamReadStrategy** | Reads from DynamoDB streams | DynamoDB streams |

### Strategy Configuration

```json
{
  "plugin": {
    "strategy": {
      "name": "Kafka.Connect.MongoDb.Strategies.UpsertStrategy"
    }
  }
}
```

## Building and Running

### Build the Project

```bash
# Build all projects
dotnet build Kafka.sln

# Build specific project
dotnet build src/Kafka.Connect/Kafka.Connect.csproj

# Build plugins
dotnet build src/Plugins/Kafka.Connect.MongoDb/Kafka.Connect.MongoDb.csproj
```

### Run the Application

#### As Leader Node

```bash
cd src/Kafka.Connect
dotnet run --mode=leader --config=/path/to/appsettings.leader.json
```

#### As Worker Node

```bash
cd src/Kafka.Connect
dotnet run --mode=worker --config=/path/to/appsettings.worker.json
```

### Run Tests

```bash
# Run all tests
dotnet test

# Run specific test project
dotnet test tests/Kafka.Connect.UnitTests/Kafka.Connect.UnitTests.csproj

# Run with coverage
dotnet test --collect:"XPlat Code Coverage"
```

## Docker Support

### Using Docker Compose

The project includes Docker support for easy deployment:

```bash
# Start all services (Kafka, databases, Kafka Connect)
docker-compose up -d

# View logs
docker-compose logs -f kafka-connect

# Stop services
docker-compose down
```

### Build Docker Image

```bash
# Build the image
docker build -t kafka-connect-dotnet:latest -f Dockerfile .

# Run as worker
docker run -d \
  -v /path/to/config:/config \
  -v /path/to/plugins:/plugins \
  kafka-connect-dotnet:latest \
  --mode=worker \
  --config=/config/appsettings.worker.json
```

### Docker Compose Example

```yaml
version: '3.8'
services:
  kafka-connect-leader:
    image: kafka-connect-dotnet:latest
    command: ["--mode=leader", "--config=/config/appsettings.leader.json"]
    volumes:
      - ./config:/config
      - ./plugins:/plugins
    environment:
      - ASPNETCORE_ENVIRONMENT=Production
    depends_on:
      - kafka

  kafka-connect-worker-1:
    image: kafka-connect-dotnet:latest
    command: ["--mode=worker", "--config=/config/appsettings.worker.json"]
    volumes:
      - ./config:/config
      - ./plugins:/plugins
    environment:
      - ASPNETCORE_ENVIRONMENT=Production
    depends_on:
      - kafka
      - kafka-connect-leader
```

## Advanced Configuration

### Fault Tolerance

Configure retry behavior and error handling:

```json
{
  "worker": {
    "faultTolerance": {
      "batches": {
        "size": 100,
        "parallelism": 50,
        "interval": 10000,
        "poll": 1000
      },
      "retries": {
        "attempts": 3,
        "interval": 1000
      },
      "errors": {
        "tolerance": "All"
      }
    }
  }
}
```

### Health Checks

Monitor connector health:

```json
{
  "worker": {
    "healthCheck": {
      "disabled": false,
      "initialDelayMs": 10000,
      "periodicDelayMs": 50000
    }
  }
}
```

### Logging

Configure structured logging with Serilog:

```json
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Information",
      "Override": {
        "Kafka.Connect.Plugin.Logging.ConnectLog": "Information",
        "Kafka.Connect.Worker": "Information"
      }
    },
    "WriteTo": [
      {
        "Name": "Console",
        "Args": {
          "formatter": "Serilog.Formatting.Json.JsonFormatter, Serilog"
        }
      }
    ]
  }
}
```

### Schema Registry

For Avro support, configure schema registry:

```json
{
  "worker": {
    "schemaRegistry": {
      "url": "http://localhost:8081",
      "connectionTimeoutMs": 5000,
      "maxCachedSchemas": 10
    },
    "converters": {
      "key": "Kafka.Connect.Converters.AvroConverter",
      "value": "Kafka.Connect.Converters.AvroConverter"
    }
  }
}
```

## Creating Custom Connectors

To create a custom connector plugin:

1. Create a new class library project
2. Reference `Kafka.Connect.Plugin`
3. Implement the required interfaces:
   - `IPluginHandler` - Main plugin handler
   - `IStrategy` - Data operation strategies
   - `IPluginInitializer` - Plugin initialization

Example structure:
```
MyCustomConnector/
├── MyCustomPluginHandler.cs
├── MyCustomCommandHandler.cs
├── DefaultPluginInitializer.cs
├── Models/
│   └── PluginConfig.cs
└── Strategies/
    ├── InsertStrategy.cs
    ├── UpdateStrategy.cs
    └── ReadStrategy.cs
```

See existing plugins in [`src/Plugins/`](src/Plugins/) for reference implementations.

## Performance Tuning

### Batch Processing

Adjust batch size and parallelism for optimal throughput:

```json
{
  "faultTolerance": {
    "batches": {
      "size": 500,        // Larger batches for higher throughput
      "parallelism": 100, // More parallel processing
      "interval": 5000,   // Shorter intervals for lower latency
      "poll": 500
    }
  }
}
```

### Task Scaling

Increase the number of tasks per connector:

```json
{
  "connectors": {
    "my-connector": {
      "tasks": 4  // Run 4 parallel tasks
    }
  }
}
```

### Worker Scaling

Deploy multiple worker nodes for horizontal scaling.

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify Kafka bootstrap servers are accessible
   - Check network connectivity to target databases
   - Validate credentials and permissions

2. **Plugin Not Found**
   - Ensure plugin DLLs are in the configured location
   - Verify plugin initializer configuration
   - Check assembly names and class names

3. **Data Not Flowing**
   - Check connector status via health endpoints
   - Review logs for errors
   - Verify topic names and configurations
   - Ensure proper permissions on topics

4. **Performance Issues**
   - Adjust batch sizes and parallelism
   - Scale workers horizontally
   - Monitor resource usage (CPU, memory, network)

### Debug Mode

Enable detailed logging:

```json
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Debug"
    }
  }
}
```

## Documentation

- [Worker Configuration](docs/appsettings.worker.md)
- [Leader Configuration](docs/appsettings.leader.md)
- [PostgreSQL Connector](docs/appsettings.postgres.connector.md)
- [SQL Server Connector](docs/appsettings.sqlserver.connector.md)
- [Oracle Connector](docs/appsettings.oracle.connector.md)
- [CI/CD Guide](README-CICD.md)

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow C# coding conventions
- Write unit tests for new features
- Update documentation as needed
- Ensure all tests pass before submitting PR

## License

This project is licensed under the terms specified in the [LICENSE](LICENSE) file.

## Support

For issues, questions, or contributions:
- Open an issue on GitHub
- Check existing documentation
- Review example configurations in [`tests/Kafka.Connect.Tests/Configurations/`](tests/Kafka.Connect.Tests/Configurations/)

## Acknowledgments

Built with:
- [.NET 8.0](https://dotnet.microsoft.com/)
- [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet)
- [Serilog](https://serilog.net/)
- Various database drivers (MongoDB.Driver, Npgsql, etc.)
