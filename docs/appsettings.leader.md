# Kafka Connect .NET Leader Configuration Documentation

This document provides a comprehensive guide to the `appsettings.leader.json` configuration file used by the Kafka Connect .NET leader node.

## Overview

The leader node in Kafka Connect .NET is responsible for coordinating tasks across worker nodes, managing connector configurations, and ensuring the overall health of the distributed system. The `appsettings.leader.json` file configures how the leader operates.

## Configuration Structure

```json
{
  "leader": {
    // Kafka connection settings
    // Internal topic configurations
    // Health check and monitoring settings
    // Fault tolerance and restart policies
    // Message conversion settings
    // Plugin configurations
  }
}
```

## Kafka Connection Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `bootstrapServers` | string | `"localhost:9092"` | Comma-separated list of Kafka broker addresses |
| `securityProtocol` | string | `"PlainText"` | Protocol used to communicate with brokers (PlainText, Ssl, SaslPlaintext, SaslSsl) |
| `sslCaLocation` | string | `""` | Path to CA certificate file for verifying the broker's key |
| `sslCertificateLocation` | string | `""` | Path to client's certificate |
| `sslKeyLocation` | string | `""` | Path to client's key |
| `sslKeyPassword` | string | `""` | Password for client's key |
| `allowAutoCreateTopics` | boolean | `true` | Allow automatic creation of topics |
| `enableAutoCommit` | boolean | `true` | Automatically commit offsets |
| `enableAutoOffsetStore` | boolean | `false` | Automatically store offsets |
| `enablePartitionEof` | boolean | `true` | Emit partition EOF events when reaching end of partition |
| `fetchWaitMaxMs` | integer | `50` | Maximum time to wait for data from broker |
| `partitionAssignmentStrategy` | string | `"RoundRobin"` | Strategy for assigning partitions to consumers (Range, RoundRobin, CooperativeSticky) |
| `autoOffsetReset` | string | `"earliest"` | Position to start reading when no offset is stored (earliest, latest) |
| `isolationLevel` | string | `"ReadUncommitted"` | Transaction isolation level (ReadUncommitted, ReadCommitted) |

## Internal Topics Configuration

```json
"topics": {
  "connector-config-topic": { "purpose": "config" },
  "__kafka_connect_dotnet_command": { "purpose": "command" }
}
```

| Topic | Purpose | Description |
|-------|---------|-------------|
| `connector-config-topic` | `config` | Stores connector configurations |
| `__kafka_connect_dotnet_command` | `command` | Used for distributing commands to workers |

The leader creates these topics automatically with appropriate partition counts:
- Command topics: 50 partitions
- Config topics: 1 partition

## Schema Registry Configuration (Optional)

```json
"schemaRegistry": {
  "connectionTimeoutMs": 5000,
  "maxCachedSchemas": 10,
  "url": "http://localhost:28081"
}
```

> **Note**: This section is optional when using JsonConverter (default). It's only required when using AvroConverter or other converters that need schema registry.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `connectionTimeoutMs` | integer | `5000` | Connection timeout in milliseconds |
| `maxCachedSchemas` | integer | `10` | Maximum number of schemas to cache |
| `url` | string | `"http://localhost:28081"` | Schema registry URL |

## Health Check Configuration

```json
"healthCheck": {
  "disabled": false,
  "initialDelayMs": 10000,
  "periodicDelayMs": 50000
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `disabled` | boolean | `false` | Disable health checks |
| `initialDelayMs` | integer | `10000` | Initial delay before first health check (ms) |
| `periodicDelayMs` | integer | `50000` | Interval between health checks (ms) |

## Failover Configuration

```json
"failOver": {
  "disabled": false,
  "failureThreshold": 3,
  "initialDelayMs": 600,
  "periodicDelayMs": 20000,
  "restartDelayMs": 20000
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `disabled` | boolean | `false` | Disable failover mechanism |
| `failureThreshold` | integer | `3` | Number of failures before triggering failover |
| `initialDelayMs` | integer | `600` | Initial delay before first failover check (ms) |
| `periodicDelayMs` | integer | `20000` | Interval between failover checks (ms) |
| `restartDelayMs` | integer | `20000` | Delay before restarting after failover (ms) |

## Restart Configuration

```json
"restarts": {
  "enabled": "Worker,Connector",
  "attempts": 5,
  "periodicDelayMs": 500,
  "stopOnFailure": true
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `enabled` | string | `"Worker,Connector"` | Comma-separated list of components that can be restarted (Worker, Connector, Task) |
| `attempts` | integer | `5` | Maximum number of restart attempts |
| `periodicDelayMs` | integer | `500` | Delay between restart attempts (ms) |
| `stopOnFailure` | boolean | `true` | Stop the component if restart attempts are exhausted |

## Fault Tolerance Configuration

```json
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
  },
  "eof": {
    "enabled": false
  }
}
```

### Batch Processing

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `batches.size` | integer | `100` | Number of records to process in a batch |
| `batches.parallelism` | integer | `50` | Degree of parallelism for batch processing |
| `batches.interval` | integer | `10000` | Interval between batch processing (ms) |
| `batches.poll` | integer | `1000` | Polling interval for new records (ms) |

### Retry Mechanism

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `retries.attempts` | integer | `3` | Maximum number of retry attempts |
| `retries.interval` | integer | `1000` | Delay between retry attempts (ms) |

### Error Handling

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `errors.tolerance` | string | `"All"` | Error tolerance level (None, All, Data) |
| `eof.enabled` | boolean | `false` | Enable end-of-file handling |

## Message Conversion

```json
"converters": {
  "key": "Kafka.Connect.Converters.JsonConverter",
  "value": "Kafka.Connect.Converters.JsonConverter"
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `key` | string | `"Kafka.Connect.Converters.JsonConverter"` | Converter class for message keys |
| `value` | string | `"Kafka.Connect.Converters.JsonConverter"` | Converter class for message values |

Available converters:
- `Kafka.Connect.Converters.JsonConverter` - JSON serialization (doesn't require schema registry)
- `Kafka.Connect.Converters.AvroConverter` - Avro serialization (requires schema registry)
- `Kafka.Connect.Converters.StringConverter` - String serialization
- `Kafka.Connect.Converters.NullConverter` - Null serialization
- `Kafka.Connect.Converters.JsonSchemaConverter` - JSON Schema serialization

## Plugin Configuration

```json
"plugins": {
  "location": "/Users/picardro/Github/external/roshan-picardo/kafka-connect-dotnet/app/bin/plugins/",
  "initializers": {
    "default-mongodb": {
      "prefix": "mongodb",
      "assembly": "Kafka.Connect.MongoDb.dll",
      "class": "Kafka.Connect.MongoDb.DefaultPluginInitializer"
    },
    "default-postgres": {
      "prefix": "postgres",
      "assembly": "Kafka.Connect.Postgres.dll",
      "class": "Kafka.Connect.Postgres.DefaultPluginInitializer"
    },
    "default-sqlserver": {
      "prefix": "sqlserver",
      "assembly": "Kafka.Connect.SqlServer.dll",
      "class": "Kafka.Connect.SqlServer.DefaultPluginInitializer"
    }
  }
}
```

| Setting | Type | Description |
|---------|------|-------------|
| `location` | string | Directory path containing plugin assemblies |
| `initializers` | object | Map of plugin initializers |

### Plugin Initializer Configuration

| Setting | Type | Description |
|---------|------|-------------|
| `prefix` | string | Prefix used to identify the plugin |
| `assembly` | string | Assembly file name |
| `class` | string | Fully qualified class name of the initializer |

## Settings Directory

```json
"settings": "/Users/picardro/Github/external/roshan-picardo/kafka-connect-dotnet/settings/"
```

| Setting | Type | Description |
|---------|------|-------------|
| `settings` | string | Directory path for additional configuration files |

## Best Practices

1. **Security**: In production environments, configure proper security settings (securityProtocol, SSL certificates).
2. **Converters**: Use JsonConverter unless you specifically need Avro or other formats.
3. **Fault Tolerance**: Adjust batch size and parallelism based on your system's capabilities.
4. **Plugin Path**: Ensure the plugin location path is accessible to the application.
5. **Topics**: In production, create topics manually with appropriate replication factors.

## Example Configuration

A minimal production-ready configuration might look like:

```json
{
  "leader": {
    "allowAutoCreateTopics": true,
    "bootstrapServers": "kafka-broker1:9092,kafka-broker2:9092,kafka-broker3:9092",
    "securityProtocol": "Ssl",
    "sslCaLocation": "/path/to/ca.pem",
    "sslCertificateLocation": "/path/to/cert.pem",
    "sslKeyLocation": "/path/to/key.pem",
    "enableAutoCommit": true,
    "enablePartitionEof": true,
    "autoOffsetReset": "earliest",
    "topics": {
      "connector-config-topic": { "purpose": "config" },
      "__kafka_connect_dotnet_command": { "purpose": "command" }
    },
    "healthCheck": {
      "disabled": false,
      "initialDelayMs": 10000,
      "periodicDelayMs": 50000
    },
    "converters": {
      "key": "Kafka.Connect.Converters.JsonConverter",
      "value": "Kafka.Connect.Converters.JsonConverter"
    },
    "plugins": {
      "location": "/opt/kafka-connect-dotnet/plugins/",
      "initializers": {
        "default-mongodb": {
          "prefix": "mongodb",
          "assembly": "Kafka.Connect.MongoDb.dll",
          "class": "Kafka.Connect.MongoDb.DefaultPluginInitializer"
        },
        "default-postgres": {
          "prefix": "postgres",
          "assembly": "Kafka.Connect.Postgres.dll",
          "class": "Kafka.Connect.Postgres.DefaultPluginInitializer"
        },
        "default-sqlserver": {
          "prefix": "sqlserver",
          "assembly": "Kafka.Connect.SqlServer.dll",
          "class": "Kafka.Connect.SqlServer.DefaultPluginInitializer"
        }
      }
    },
    "settings": "/opt/kafka-connect-dotnet/settings/"
  }
}