# Kafka Connect .NET

[![.NET](https://github.com/roshan-picardo/kafka-connect-dotnet/actions/workflows/dotnet.yml/badge.svg)](https://github.com/roshan-picardo/kafka-connect-dotnet/actions/workflows/dotnet.yml)

A .NET implementation of the Kafka Connect framework for building data pipelines between Apache Kafka and external systems.

---

## What is this?

Kafka Connect .NET lets you move data between Kafka and databases or other systems without writing custom consumer/producer code. You configure connectors in JSON files; the framework handles polling, batching, retries, offset management, and fault tolerance.

The framework follows the same conceptual model as Apache Kafka Connect:

- **Source connectors** read from an external system and publish records to Kafka topics.
- **Sink connectors** consume records from Kafka topics and write them to an external system.
- **Workers** are the processes that execute connector tasks.
- **The leader** (in distributed mode) coordinates which connectors run on which workers.

---

## Supported plugins

| Plugin | Source | Sink |
|--------|--------|------|
| MongoDB | Change Streams, cursor-based read | Insert, Update, Delete, Upsert |
| PostgreSQL | Changelog table + snapshot | Insert, Update, Delete, Upsert |
| SQL Server | Changelog table + snapshot | Insert, Update, Delete, Upsert |
| MySQL | Changelog table + snapshot | Insert, Update, Delete, Upsert |
| MariaDB | Changelog table + snapshot | Insert, Update, Delete, Upsert |
| Oracle | Changelog table + snapshot | Insert, Update, Delete, Upsert |
| DynamoDB | Scan/query, DynamoDB Streams | Insert, Update, Delete, Upsert |
| Cassandra | Cursor-based read + snapshot | Insert, Update, Delete, Upsert |

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  Kafka Cluster                    │
│  ┌───────────┐  ┌───────────┐  ┌─────────────┐   │
│  │  Topic A  │  │  Topic B  │  │ Config topic│   │
│  └───────────┘  └───────────┘  └─────────────┘   │
└──────────────────────────────────────────────────┘
           ▲  │                        ▲  │
           │  ▼                        │  │
┌──────────────────┐         ┌─────────────────────┐
│  Source Connector│         │      Leader          │
│  (reads external │         │  (distributes config │
│   system, writes │         │   to workers via     │
│   to Kafka)      │         │   config topic)      │
└──────────────────┘         └─────────────────────┘
           ▲  │                        │
           │  ▼                        ▼
┌──────────────────┐         ┌─────────────────────┐
│  Sink Connector  │         │  Worker 1 / Worker N │
│  (reads Kafka,   │         │  (runs connector     │
│   writes to      │         │   tasks)             │
│   external system│         └─────────────────────┘
└──────────────────┘
```

In **standalone mode** the worker manages its connectors directly from its own config file — no leader required.

In **distributed mode** the leader publishes connector configurations to a Kafka topic. Workers subscribe to that topic and start or stop connectors based on their assignments. This allows horizontal scaling and automatic fail-over.

---

## Getting started

### Prerequisites

- .NET 8.0 or later
- Apache Kafka
- At least one supported database or storage system

### Build

```bash
dotnet build Kafka.sln
```

### Run

**Standalone (single worker, no leader):**

```bash
dotnet run --project src/Kafka.Connect --mode=worker --config=appsettings.json
```

**Distributed (leader + workers):**

```bash
# Start the leader
dotnet run --project src/Kafka.Connect --mode=leader --config=appsettings.leader.json

# Start one or more workers
dotnet run --project src/Kafka.Connect --mode=worker --config=appsettings.worker.json
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [Base Configuration](docs/connect-base.md) | All configuration fields for leader and worker nodes |
| [Deployment Modes](docs/deployment-modes.md) | Standalone vs distributed mode explained with examples |
| [Connectors](docs/connectors.md) | Source and sink connector concepts, processors, and configuration fields |
| [MongoDB Plugin](docs/plugins/mongodb.md) | MongoDB-specific configuration and examples |
| [Cassandra Plugin](docs/plugins/cassandra.md) | Cassandra-specific configuration and examples |
| [PostgreSQL Plugin](docs/plugins/postgres.md) | PostgreSQL-specific configuration and examples |
| [SQL Server Plugin](docs/plugins/sqlserver.md) | SQL Server-specific configuration and examples |
| [Oracle Plugin](docs/plugins/oracle.md) | Oracle-specific configuration and examples |
| [MySQL Plugin](docs/plugins/mysql.md) | MySQL-specific configuration and examples |
| [MariaDB Plugin](docs/plugins/mariadb.md) | MariaDB-specific configuration and examples |
| [DynamoDB Plugin](docs/plugins/dynamodb.md) | DynamoDB-specific configuration and examples |

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes and open a pull request

Please write unit tests for new functionality and ensure existing tests pass before submitting.

## License

See the [LICENSE](LICENSE) file.
