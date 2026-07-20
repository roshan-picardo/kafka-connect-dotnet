# MongoDB Plugin

The MongoDB plugin supports both **sink** and **source** connectors.

- **Sink** — writes Kafka records to a MongoDB collection using insert, update, delete, or upsert operations.
- **Source** — reads change events from a MongoDB collection using Change Streams (`StreamsReadStrategy`) or reads documents using a cursor-based approach (`ReadStrategy`).

---

## Plugin registration

Add the following to `plugins.initializers` in your worker configuration:

```json
"mongodb": {
  "assembly": "Kafka.Connect.MongoDb.dll",
  "class": "Kafka.Connect.MongoDb.DefaultPluginInitializer"
}
```

---

## Connection properties

These fields go inside `plugin.properties`:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `connectionUri` | string | Yes* | — | MongoDB connection URI. Supports the format string `{0}` for username, `{1}` for URL-encoded password, and `{2}` for database name if you want them interpolated from the separate fields. |
| `username` | string | No | — | Username, interpolated into `connectionUri` at position `{0}`. |
| `password` | string | No | — | Base64-encoded password, interpolated into `connectionUri` at position `{1}` after URL decoding. |
| `database` | string | Yes | — | Target database name. |
| `collection` | string | Sink | — | Target collection name for sink connectors. |
| `isWriteOrdered` | bool | No | `true` | Whether bulk write operations maintain insertion order. Set to `false` for higher throughput when order does not matter. |
| `filter` | string | Sink (update/upsert/delete) | — | MongoDB filter document as a JSON string. Use `#fieldName#` as a placeholder for values from the Kafka record (e.g. `{"userId": "#userId#"}`). Required by `UpdateStrategy`, `DeleteStrategy`, and `UpsertStrategy`. |
| `commands` | map | Source | — | Named command definitions for source connectors. Each entry describes how to read from a collection. |

---

## Sink connector

### Strategies

| Strategy class | Description |
|----------------|-------------|
| `Kafka.Connect.MongoDb.Strategies.InsertStrategy` | Inserts each record as a new document. |
| `Kafka.Connect.MongoDb.Strategies.UpdateStrategy` | Updates documents matched by `filter`. |
| `Kafka.Connect.MongoDb.Strategies.DeleteStrategy` | Deletes documents matched by `filter`. |
| `Kafka.Connect.MongoDb.Strategies.UpsertStrategy` | Replaces a matched document or inserts it if not found. |

### Example — upsert sink

```json
{
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
```

---

## Source connector

### Strategies

| Strategy class | Description |
|----------------|-------------|
| `Kafka.Connect.MongoDb.Strategies.StreamsReadStrategy` | Watches a MongoDB change stream for real-time change events. |
| `Kafka.Connect.MongoDb.Strategies.ReadStrategy` | Polls a collection using a cursor/timestamp column for incremental reads. |

### Command configuration

Source connectors define one or more named `commands`. Each command specifies a collection to read and the Kafka topic to publish to.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | string | Yes | Kafka topic to publish records to. |
| `collection` | string | Yes | MongoDB collection to read from. |
| `keys` | string[] | Yes | Fields that uniquely identify a document. Used to build the Kafka message key. |
| `timestamp` | long | No | Unix epoch milliseconds of the last processed record. Maintained by the connector — do not set manually after initial configuration. |
| `filters` | map | No | Additional filter criteria applied when querying the collection. |

### Example — change stream source

```json
{
  "mongodb-source-connector": {
    "groupId": "mongodb-source-connector",
    "tasks": 1,
    "plugin": {
      "type": "source",
      "name": "mongodb",
      "handler": "Kafka.Connect.MongoDb.MongoPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.MongoDb.Strategies.StreamsReadStrategy"
      },
      "properties": {
        "connectionUri": "mongodb://localhost:27017/?replicaSet=rs0",
        "database": "mydb",
        "commands": {
          "read-users": {
            "topic": "user-changes",
            "collection": "users",
            "keys": ["userId"],
            "timestamp": 0
          }
        }
      }
    }
  }
}
```

> MongoDB change streams require a replica set or sharded cluster. Standalone `mongod` instances do not support change streams.
