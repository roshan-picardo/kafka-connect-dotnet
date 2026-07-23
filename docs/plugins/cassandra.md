# Cassandra Plugin

The Cassandra plugin supports both **sink** and **source** connectors.

- **Sink** — writes Kafka records to a Cassandra table using insert, update, delete, or upsert operations.
- **Source** — reads rows from a Cassandra table using a cursor-based `ReadStrategy` with optional initial snapshot support.

---

## Plugin registration

Add the following to `plugins.initializers` in your worker configuration:

```json
"cassandra": {
  "assembly": "Kafka.Connect.Cassandra.dll",
  "class": "Kafka.Connect.Cassandra.DefaultPluginInitializer"
}
```

---

## Connection properties

These fields go inside `plugin.properties`:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `hosts` | string[] | Yes | — | List of Cassandra contact points (IP addresses or hostnames). |
| `port` | int | No | `9042` | CQL native transport port. |
| `keyspace` | string | Yes | — | Default Cassandra keyspace. Used when a command does not specify its own `keyspace`. |
| `table` | string | Sink | — | Target table name for sink operations when not defined per-command. |
| `filter` | string | Sink (update/upsert/delete) | — | CQL WHERE clause fragment used to identify the target row. Use `#fieldName#` placeholders (e.g. `userId = '#userId#'`). |
| `lookup` | string | Sink | — | Alternative lookup expression for strategies that distinguish lookup from filter. |
| `commands` | map | Source | — | Named command definitions for source connectors. |

---

## Sink connector

### Strategies

| Strategy class | Description |
|----------------|-------------|
| `Kafka.Connect.Cassandra.Strategies.InsertStrategy` | Inserts each record as a new row. |
| `Kafka.Connect.Cassandra.Strategies.UpdateStrategy` | Updates rows matched by `filter`. |
| `Kafka.Connect.Cassandra.Strategies.DeleteStrategy` | Deletes rows matched by `filter`. |
| `Kafka.Connect.Cassandra.Strategies.UpsertStrategy` | Inserts or updates using CQL `INSERT ... IF NOT EXISTS` / `UPDATE` semantics. |

### Example — upsert sink

```json
{
  "cassandra-sink-connector": {
    "groupId": "cassandra-sink-connector",
    "topics": ["user-events"],
    "tasks": 1,
    "plugin": {
      "type": "sink",
      "name": "cassandra",
      "handler": "Kafka.Connect.Cassandra.CassandraPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.Cassandra.Strategies.UpsertStrategy"
      },
      "properties": {
        "hosts": ["127.0.0.1"],
        "port": 9042,
        "keyspace": "myapp",
        "table": "users"
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
| `Kafka.Connect.Cassandra.Strategies.ReadStrategy` | Polls a table using a cursor column. Supports an optional initial snapshot. |

### Command configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | string | Yes | Kafka topic to publish records to. |
| `table` | string | Yes | Cassandra table to read from. |
| `keyspace` | string | No | Keyspace for this command. Overrides the top-level `keyspace`. |
| `keys` | string[] | Yes | Partition/clustering key columns used to uniquely identify a row and build the Kafka message key. |
| `filters` | map | No | Additional equality filters applied to the query. |
| `snapshot.enabled` | bool | No | `false` | When `true`, performs a full table scan before switching to incremental polling. |
| `snapshot.key` | string | No | — | Column used to page through the snapshot (e.g. a partition key). |

> `snapshot.id`, `snapshot.total`, and `snapshot.timestamp` are managed by the connector and track snapshot progress. Do not set these manually.

### Example — source with snapshot

```json
{
  "cassandra-source-connector": {
    "groupId": "cassandra-source-connector",
    "tasks": 1,
    "plugin": {
      "type": "source",
      "name": "cassandra",
      "handler": "Kafka.Connect.Cassandra.CassandraPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.Cassandra.Strategies.ReadStrategy"
      },
      "properties": {
        "hosts": ["127.0.0.1"],
        "port": 9042,
        "keyspace": "myapp",
        "commands": {
          "read-users": {
            "topic": "user-changes",
            "table": "users",
            "keyspace": "myapp",
            "keys": ["userId"],
            "filters": {},
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
```
