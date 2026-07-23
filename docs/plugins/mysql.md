# MySQL Plugin

The MySQL plugin supports both **sink** and **source** connectors.

- **Sink** — writes Kafka records to a MySQL table using insert, update, delete, or upsert operations.
- **Source** — reads rows from a MySQL table using a changelog (audit) table pattern with optional initial snapshot support.

---

## Plugin registration

Add the following to `plugins.initializers` in your worker configuration:

```json
"mysql": {
  "assembly": "Kafka.Connect.MySql.dll",
  "class": "Kafka.Connect.MySql.DefaultPluginInitializer"
}
```

---

## Connection properties

These fields go inside `plugin.properties`:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `host` | string | Yes | — | MySQL server hostname or IP address. |
| `port` | int | No | `3306` | MySQL server port. |
| `userId` | string | Yes | — | Database user. |
| `password` | string | Yes | — | Database password. |
| `database` | string | Yes | — | Database name. |
| `schema` | string | No | `mysql` | Default schema. |
| `table` | string | Sink | — | Target table name for sink operations. |
| `filter` | string | Sink (update/upsert/delete) | — | SQL WHERE clause fragment. Use `#fieldName#` placeholders (e.g. `` `userId` = '#userId#' ``). |
| `lookup` | string | Sink | — | Alternative lookup expression. |
| `changelog` | object | Source | — | Changelog table configuration for `ReadStrategy`. |
| `commands` | map | Source | — | Named command definitions for source connectors. |

### Changelog configuration (source)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `changelog.schema` | string | No | `mysql` | Schema of the changelog table. |
| `changelog.table` | string | Yes | — | Name of the changelog table. |
| `changelog.retention` | int | No | `1` | Days to retain processed changelog records. |

---

## Sink connector

### Strategies

| Strategy class | Description |
|----------------|-------------|
| `Kafka.Connect.MySql.Strategies.InsertStrategy` | Inserts each record as a new row. |
| `Kafka.Connect.MySql.Strategies.UpdateStrategy` | Updates rows matched by `filter`. |
| `Kafka.Connect.MySql.Strategies.DeleteStrategy` | Deletes rows matched by `filter`. |
| `Kafka.Connect.MySql.Strategies.UpsertStrategy` | Inserts or updates using `INSERT ... ON DUPLICATE KEY UPDATE`. |

### Example — upsert sink

```json
{
  "mysql-sink-connector": {
    "groupId": "mysql-sink-connector",
    "topics": ["user-events"],
    "tasks": 1,
    "plugin": {
      "type": "sink",
      "name": "mysql",
      "handler": "Kafka.Connect.MySql.MySqlPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.MySql.Strategies.UpsertStrategy"
      },
      "properties": {
        "host": "localhost",
        "port": 3306,
        "userId": "root",
        "password": "secret",
        "database": "mydb",
        "schema": "mydb",
        "table": "users",
        "filter": "`userId` = '#userId#'"
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
| `Kafka.Connect.MySql.Strategies.ReadStrategy` | Polls the changelog table and fetches corresponding rows. |

### Command configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | string | Yes | Kafka topic to publish records to. |
| `table` | string | Yes | Source table to read from. |
| `schema` | string | No | Schema for this command's table. Defaults to `mysql`. |
| `keys` | string[] | Yes | Columns that uniquely identify a row. |
| `filters` | map | No | Additional equality filters. |
| `snapshot.enabled` | bool | No | When `true`, performs a full table scan first. |
| `snapshot.key` | string | No | Column used to page through the snapshot. |

### Example — source with changelog

```json
{
  "mysql-source-connector": {
    "groupId": "mysql-source-connector",
    "tasks": 1,
    "plugin": {
      "type": "source",
      "name": "mysql",
      "handler": "Kafka.Connect.MySql.MySqlPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.MySql.Strategies.ReadStrategy"
      },
      "properties": {
        "host": "localhost",
        "port": 3306,
        "userId": "root",
        "password": "secret",
        "database": "mydb",
        "changelog": {
          "schema": "mydb",
          "table": "audit_log",
          "retention": 1
        },
        "commands": {
          "read-users": {
            "topic": "user-changes",
            "table": "users",
            "schema": "mydb",
            "keys": ["userId"],
            "snapshot": {
              "enabled": false,
              "key": "userId"
            }
          }
        }
      }
    }
  }
}
```
