# Base Configuration

All nodes — whether running as a **leader** or a **worker** — share a common base configuration rooted at either the `leader` or `worker` key in `appsettings.json`. This page describes every field that applies to both, plus the fields that are specific to each mode.

---

## Common Node Settings

These fields are available on both the `leader` and `worker` configuration objects.

### Identity

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | `NODE_NAME` env var or machine hostname | Unique name for this node. Used to identify the node in the cluster and in logs. |

### Kafka Connectivity

The node config extends Confluent's `ConsumerConfig`, so every standard Confluent consumer property can be set here. The most commonly used ones are:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bootstrapServers` | string | — | Comma-separated list of Kafka broker addresses (e.g. `localhost:9092`). |
| `securityProtocol` | string | `PlainText` | Kafka security protocol: `PlainText`, `Ssl`, `SaslPlaintext`, or `SaslSsl`. |
| `enableAutoCommit` | bool | `true` | Whether to automatically commit offsets. |
| `enableAutoOffsetStore` | bool | `false` | Whether offsets are stored automatically before the commit. Set to `false` to control offset storage manually. |
| `enablePartitionEof` | bool | `true` | Whether to emit an EOF event when a partition has been fully consumed. |
| `fetchWaitMaxMs` | int | `50` | Maximum time (ms) the broker waits before responding to a fetch request when there is not enough data. |
| `autoOffsetReset` | string | `earliest` | What to do when there is no initial offset or the current offset no longer exists: `earliest`, `latest`, or `error`. |
| `maxPollIntervalMs` | int | `300000` | Maximum time (ms) between two consecutive poll calls before the consumer is considered dead. |
| `partitionAssignmentStrategy` | string | — | Partition assignment strategy: `RoundRobin`, `Range`, or `CooperativeSticky`. |
| `isolationLevel` | string | `ReadUncommitted` | Transaction isolation: `ReadUncommitted` or `ReadCommitted`. |
| `allowAutoCreateTopics` | bool | `false` | Whether the consumer is allowed to auto-create topics it subscribes to. |

### Internal Topics

| Field | Type | Description |
|-------|------|-------------|
| `topics.config` | string | Name of the internal Kafka topic used to distribute connector configuration between the leader and workers. Defaults to `__kafka_connect_dotnet_settings`. |
| `topics.command` | string | Name of the internal Kafka topic used for operational commands (pause, resume, etc.). Defaults to `__kafka_connect_dotnet_command`. |

### Schema Registry

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `schemaRegistry.url` | string | — | URL of the Confluent Schema Registry (required when using `AvroConverter` or `JsonSchemaConverter`). |
| `schemaRegistry.connectionTimeoutMs` | int | `5000` | Connection timeout in milliseconds. |
| `schemaRegistry.maxCachedSchemas` | int | `10` | Number of schemas to cache locally. |

### Converters

Converters handle serialisation and deserialisation of Kafka message keys and values.

| Field | Type | Description |
|-------|------|-------------|
| `converters.key` | string | Fully-qualified class name for the key converter. |
| `converters.value` | string | Fully-qualified class name for the value converter. |
| `converters.subject` | string | Schema subject name (used with Avro / JSON Schema converters). |
| `converters.record` | string | Record-level converter override. |

Built-in converters:

| Class | Description |
|-------|-------------|
| `Kafka.Connect.Converters.JsonConverter` | Deserialises messages as raw JSON. Default for most use cases. |
| `Kafka.Connect.Converters.AvroConverter` | Avro serialisation using Schema Registry. |
| `Kafka.Connect.Converters.JsonSchemaConverter` | JSON Schema validation using Schema Registry. |
| `Kafka.Connect.Converters.StringConverter` | Treats message payload as a plain string. |
| `Kafka.Connect.Converters.NullConverter` | No-op converter — passes the raw bytes through. |

### Plugins

| Field | Type | Description |
|-------|------|-------------|
| `plugins.location` | string | File-system path to the directory containing plugin DLLs. |
| `plugins.initializers.<name>.folder` | string | Optional sub-folder inside `location` for this plugin's DLLs. |
| `plugins.initializers.<name>.assembly` | string | DLL file name (e.g. `Kafka.Connect.MongoDb.dll`). |
| `plugins.initializers.<name>.class` | string | Fully-qualified class name of the plugin initializer (e.g. `Kafka.Connect.MongoDb.DefaultPluginInitializer`). |

The `<name>` key is arbitrary but should match the `plugin.name` value used in connector configurations so the framework can resolve the correct plugin at runtime.

### Health Check

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `healthCheck.disabled` | bool | `false` | Set to `true` to disable the health check background service. |
| `healthCheck.timeout` | int | `60000` | Timeout in milliseconds for a single health check probe. |
| `healthCheck.interval` | int | `60000` | How often (ms) the health check runs. |

### Restarts

Controls automatic restart behaviour when connectors or tasks fail.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `restarts.enabledFor` | flags | `None` | Comma-separated list of levels to restart automatically: `Worker`, `Connector`, `Task`, `All`, or `None`. |
| `restarts.attempts` | int | `0` | Maximum restart attempts before giving up. |
| `restarts.periodicDelayMs` | int | `2000` | Delay (ms) between restart checks. |
| `restarts.retryWaitTimeMs` | int | `30000` | How long to wait (ms) before attempting to restart after a failure. |

### Fault Tolerance

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `faultTolerance.batches.size` | int | — | Maximum number of records in a single processing batch. |
| `faultTolerance.batches.parallelism` | int | — | Number of records processed in parallel within a batch. |
| `faultTolerance.batches.interval` | int | — | Time (ms) to wait between batch cycles when no records arrive. |
| `faultTolerance.batches.poll` | int | — | Kafka consumer poll timeout (ms). |
| `faultTolerance.retries.attempts` | int | — | Number of times to retry a failed operation before marking it as an error. |
| `faultTolerance.retries.interval` | int | — | Delay (ms) between retry attempts. |
| `faultTolerance.errors.tolerance` | enum | `None` | Error tolerance policy: `None` (fail on first error), `Data` (skip data errors), or `All` (skip all errors). |
| `faultTolerance.errors.exceptions` | string[] | — | List of exception type names to handle under the tolerance policy. |
| `faultTolerance.errors.topic` | string | — | Dead-letter topic name. Errors that are tolerated are published here. |
| `faultTolerance.eof.enabled` | bool | `false` | When `true`, the connector stops polling and signals end-of-stream after all partitions reach EOF. |
| `faultTolerance.eof.topic` | string | — | Topic to publish an EOF signal message to when EOF is reached. |

### Fail-Over

Controls the fail-over monitor that detects dead nodes and reassigns their work.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `failOver.disabled` | bool | `false` | Set to `true` to disable fail-over monitoring. |
| `failOver.initialDelayMs` | int | `60000` | Delay (ms) before the fail-over monitor starts after node startup. |
| `failOver.periodicDelayMs` | int | `10000` | How often (ms) the fail-over monitor checks for dead nodes. |
| `failOver.failureThreshold` | int | `20` | Number of consecutive missed heartbeats before a node is considered dead. |
| `failOver.restartDelayMs` | int | `10000` | Delay (ms) before reassigned connectors are started on the new node. |

---

## Leader-Specific Settings

The leader node is configured under the `leader` root key.

| Field | Type | Description |
|-------|------|-------------|
| `settings` | string | Path to a directory containing per-connector JSON files used in **distributed mode**. The leader reads these files and publishes them to the config topic. See [Deployment Modes](deployment-modes.md). |

**Example `appsettings.leader.json`:**

```json
{
  "urls": "http://localhost:7000/",
  "leader": {
    "name": "kafka-connect-leader",
    "bootstrapServers": "localhost:9092",
    "securityProtocol": "PlainText",
    "enableAutoCommit": true,
    "enableAutoOffsetStore": false,
    "enablePartitionEof": true,
    "fetchWaitMaxMs": 50,
    "autoOffsetReset": "earliest",
    "maxPollIntervalMs": 300000,
    "settings": "./settings-leader",
    "topics": {
      "config": "__kafka_connect_dotnet_settings",
      "command": "__kafka_connect_dotnet_command"
    },
    "converters": {
      "key": "Kafka.Connect.Converters.JsonConverter",
      "value": "Kafka.Connect.Converters.JsonConverter"
    },
    "restarts": {
      "enabledFor": "Leader,Connector",
      "attempts": 5,
      "periodicDelayMs": 500
    },
    "faultTolerance": {
      "retries": {
        "attempts": 3,
        "interval": 1000
      }
    }
  }
}
```

---

## Worker-Specific Settings

The worker node is configured under the `worker` root key.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `standalone` | bool | `true` | Set to `false` for distributed mode. In standalone mode the worker manages its own connectors without a leader. See [Deployment Modes](deployment-modes.md). |
| `settings` | string | — | Path to a directory containing per-connector JSON files used in **distributed mode** (when `standalone: false`). |

**Example `appsettings.worker.json`:**

```json
{
  "urls": "http://localhost:8000/",
  "worker": {
    "name": "kafka-connect-worker",
    "standalone": false,
    "settings": "./settings-worker",
    "bootstrapServers": "localhost:9092",
    "securityProtocol": "PlainText",
    "enableAutoCommit": true,
    "enableAutoOffsetStore": false,
    "enablePartitionEof": true,
    "fetchWaitMaxMs": 50,
    "autoOffsetReset": "earliest",
    "topics": {
      "config": "__kafka_connect_dotnet_settings",
      "command": "__kafka_connect_dotnet_command"
    },
    "healthCheck": {
      "disabled": false,
      "timeout": 60000,
      "interval": 60000
    },
    "restarts": {
      "enabledFor": "Worker,Connector",
      "attempts": 5,
      "periodicDelayMs": 500
    },
    "faultTolerance": {
      "batches": {
        "size": 100,
        "parallelism": 1,
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
    },
    "converters": {
      "key": "Kafka.Connect.Converters.JsonConverter",
      "value": "Kafka.Connect.Converters.JsonConverter"
    },
    "plugins": {
      "location": "/path/to/plugins/",
      "initializers": {
        "mongodb": {
          "assembly": "Kafka.Connect.MongoDb.dll",
          "class": "Kafka.Connect.MongoDb.DefaultPluginInitializer"
        }
      }
    }
  }
}
```

---

## Logging

Logging is configured via Serilog at the root of `appsettings.json`, independent of the `leader`/`worker` sections.

```json
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Information",
      "Override": {
        "System": "Warning",
        "Microsoft": "Error",
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
    ],
    "Enrich": ["FromLogContext"]
  }
}
```

Set `"Default": "Debug"` to enable verbose output for troubleshooting.
