# PostgreSQL Plugin

The PostgreSQL plugin supports both **sink** and **source** connectors.

- **Sink** — writes Kafka records to a PostgreSQL table using insert, update, delete, or upsert operations.
- **Source** — reads rows from a PostgreSQL table using a changelog (audit) table pattern with optional initial snapshot support.

---

## Plugin registration

Add the following to `plugins.initializers` in your worker configuration:

```json
"postgres": {
  "assembly": "Kafka.Connect.Postgres.dll",
  "class": "Kafka.Connect.Postgres.DefaultPluginInitializer"
}
```

---

## Connection properties

These fields go inside `plugin.properties`:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `host` | string | Yes | — | PostgreSQL server hostname or IP address. |
| `port` | int | No | `5432` | PostgreSQL server port. |
| `userId` | string | Yes | — | Database user. |
| `password` | string | Yes | — | Database password. |
| `database` | string | Yes | — | Database name. |
| `schema` | string | No | `public` | Default schema for tables. Used when a command does not specify its own `schema`. |
| `table` | string | Sink | — | Target table name for sink operations. |
| `filter` | string | Sink (update/upsert/delete) | — | SQL WHERE clause fragment to identify the target row. Use `#fieldName#` placeholders (e.g. `"userId" = '#userId#'`). |
| `lookup` | string | Sink | — | Alternative lookup expression for strategies that distinguish lookup from filter. |
| `changelog` | object | Source | — | Changelog table configuration used by `ReadStrategy`. |
| `commands` | map | Source | — | Named command definitions for source connectors. |

### Changelog configuration (source)

The source `ReadStrategy` relies on a changelog (audit) table that records changes to monitored tables.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `changelog.schema` | string | No | `public` | Schema of the changelog table. |
| `changelog.table` | string | Yes | — | Name of the changelog table. |
| `changelog.retention` | int | No | `1` | Number of days to retain processed changelog records. |

---

## Sink connector

### Strategies

| Strategy class | Description |
|----------------|-------------|
| `Kafka.Connect.Postgres.Strategies.InsertStrategy` | Inserts each record as a new row. |
| `Kafka.Connect.Postgres.Strategies.UpdateStrategy` | Updates rows matched by `filter`. |
| `Kafka.Connect.Postgres.Strategies.DeleteStrategy` | Deletes rows matched by `filter`. |
| `Kafka.Connect.Postgres.Strategies.UpsertStrategy` | Inserts or updates using `INSERT ... ON CONFLICT DO UPDATE`. |

### Example — upsert sink

```json
{
  "postgres-sink-connector": {
    "groupId": "postgres-sink-connector",
    "topics": ["user-events"],
    "tasks": 1,
    "plugin": {
      "type": "sink",
      "name": "postgres",
      "handler": "Kafka.Connect.Postgres.PostgresPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.Postgres.Strategies.UpsertStrategy"
      },
      "properties": {
        "host": "localhost",
        "port": 5432,
        "userId": "postgres",
        "password": "secret",
        "database": "mydb",
        "schema": "public",
        "table": "users",
        "filter": "\"userId\" = '#userId#'"
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
| `Kafka.Connect.Postgres.Strategies.ReadStrategy` | Polls the changelog table for new entries and fetches the corresponding rows from the source table. |

### Command configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | string | Yes | Kafka topic to publish records to. |
| `table` | string | Yes | Source table to read from. |
| `schema` | string | No | Schema for this command's table. Defaults to `public`. |
| `keys` | string[] | Yes | Columns that uniquely identify a row. Used to build the Kafka message key. |
| `filters` | map | No | Additional equality filters applied to the query. |
| `snapshot.enabled` | bool | No | When `true`, performs a full table scan before switching to changelog polling. |
| `snapshot.key` | string | No | Column used to page through the snapshot. |

> `snapshot.id`, `snapshot.total`, and `snapshot.timestamp` are managed by the connector. Do not set these manually.

### Example — source with changelog

```json
{
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
        "password": "secret",
        "database": "mydb",
        "changelog": {
          "schema": "public",
          "table": "audit_log",
          "retention": 1
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
```
