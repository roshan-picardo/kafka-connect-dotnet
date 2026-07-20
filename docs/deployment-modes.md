# Deployment Modes

Kafka Connect .NET can run in two modes: **standalone** and **distributed**. The mode affects how connector configurations are managed and how work is distributed across processes.

---

## Standalone Mode

In standalone mode a single worker process manages all connectors directly. There is no leader node and no coordination over Kafka. Connector configurations live inside the worker's own `appsettings.json` under the `worker.connectors` map.

**When to use:** Local development, single-machine deployments, or pipelines that don't need horizontal scaling.

### How it works

```
appsettings.json
└── worker
    ├── standalone: true
    └── connectors
        ├── my-sink-connector  →  SinkTask → external system
        └── my-source-connector → SourceTask → Kafka topic
```

1. The worker starts, reads the `connectors` map from its config file.
2. Each connector entry is started as one or more tasks within the same process.
3. Connector state is entirely local — there is no config topic and no leader.

### Configuration

Set `standalone: true` (or omit it, as it defaults to `true`) and define connectors inline:

```json
{
  "worker": {
    "standalone": true,
    "name": "standalone-worker",
    "bootstrapServers": "localhost:9092",
    "plugins": {
      "location": "/path/to/plugins/",
      "initializers": {
        "mongodb": {
          "assembly": "Kafka.Connect.MongoDb.dll",
          "class": "Kafka.Connect.MongoDb.DefaultPluginInitializer"
        }
      }
    },
    "connectors": {
      "mongodb-sink-connector": {
        "groupId": "standalone-mongodb-sink-connector",
        "topics": ["events"],
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
            "collection": "events",
            "filter": "{\"eventId\": \"#eventId#\"}"
          }
        }
      }
    }
  }
}
```

### Running

```bash
dotnet run --project src/Kafka.Connect \
  --mode=worker \
  --config=appsettings.json
```

---

## Distributed Mode

In distributed mode a **leader** node and one or more **worker** nodes cooperate via Kafka. The leader is responsible for publishing connector configurations to a shared Kafka topic; workers consume that topic and start or stop connectors accordingly.

**When to use:** Production deployments, multi-node scaling, high-availability setups.

### How it works

```
Leader process
├── Reads connector JSON files from settings-leader/ directory
├── Publishes configurations to __kafka_connect_dotnet_settings topic
└── Monitors worker heartbeats for fail-over

Worker process(es)
├── standalone: false
├── Subscribes to __kafka_connect_dotnet_settings topic
├── Receives connector configurations from the leader
└── Starts/stops connectors and their tasks accordingly
```

The leader publishes each connector configuration as a message keyed by connector name. Workers maintain a consumer group on the config topic so that each worker receives the full set of assignments. The `workers` array in each connector file controls which worker nodes are assigned that connector.

### Leader configuration

The leader reads per-connector JSON files from the directory specified by `settings`. Each file in that directory describes one or more connectors and specifies which workers should run them.

**`appsettings.leader.json`:**

```json
{
  "urls": "http://localhost:7000/",
  "leader": {
    "name": "kafka-connect-leader",
    "bootstrapServers": "localhost:9092",
    "settings": "./settings-leader",
    "topics": {
      "config": "__kafka_connect_dotnet_settings",
      "command": "__kafka_connect_dotnet_command"
    },
    "converters": {
      "key": "Kafka.Connect.Converters.JsonConverter",
      "value": "Kafka.Connect.Converters.JsonConverter"
    }
  }
}
```

**`settings-leader/my-connector.json`** (distributed connector definition):

```json
{
  "workers": ["kafka-connect-worker"],
  "my-sink-connector": {
    "groupId": "my-sink-connector",
    "topics": ["events"],
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
        "collection": "events",
        "filter": "{\"eventId\": \"#eventId#\"}"
      }
    }
  }
}
```

The top-level `workers` array lists the names of worker nodes that should run the connectors defined in this file. Each connector is keyed by its name at the same level as `workers`.

### Worker configuration

Workers in distributed mode set `standalone: false` and optionally specify a `settings` directory for local overrides. The primary source of connector config, however, comes from the leader via the Kafka config topic.

**`appsettings.worker.json`:**

```json
{
  "urls": "http://localhost:8000/",
  "worker": {
    "name": "kafka-connect-worker",
    "standalone": false,
    "bootstrapServers": "localhost:9092",
    "topics": {
      "config": "__kafka_connect_dotnet_settings",
      "command": "__kafka_connect_dotnet_command"
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

### Running

Start the leader first, then one or more workers:

```bash
# Leader
dotnet run --project src/Kafka.Connect \
  --mode=leader \
  --config=appsettings.leader.json

# Worker (repeat for each worker node)
dotnet run --project src/Kafka.Connect \
  --mode=worker \
  --config=appsettings.worker.json
```

### Fail-over

When a worker node stops responding, the leader's fail-over monitor detects the absence of heartbeats and reassigns that worker's connectors to the remaining healthy workers. The `failOver` configuration block on the leader controls how quickly this detection and reassignment happens. See [Base Configuration](connect-base.md#fail-over) for the relevant fields.

---

## Comparison

| | Standalone | Distributed |
|---|---|---|
| Number of processes | 1 (worker only) | 1 leader + N workers |
| Config location | `worker.connectors` in appsettings | JSON files in `settings-leader/` directory |
| Horizontal scaling | No | Yes |
| Fail-over | No | Yes (via leader) |
| `standalone` flag | `true` | `false` |
| Leader required | No | Yes |
