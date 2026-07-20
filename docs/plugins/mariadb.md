# MariaDB Plugin

The MariaDB plugin supports both **sink** and **source** connectors.

- **Sink** — writes Kafka records to a MariaDB table using insert, update, delete, or upsert operations.
- **Source** — reads rows from a MariaDB table using a changelog (audit) table pattern with optional initial snapshot support.

---

## Plugin registration

Add the following to `plugins.initializers` in your worker configuration:

```json
"mariadb": {
  "assembly": "Kafka.Connect.MariaDb.dll",
  "class": "Kafka.Connect.MariaDb.DefaultPluginInitializer"
}
```

---

## Connection properties

These fields go inside `plugin.properties`:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `host` | string | Yes | — | MariaDB server hostname or IP address. |
| `port` | int | No | `3306` | MariaDB server port. |
| `userId` | string | Yes | — | Database user. |
| `password` | string | Yes | — | Database password. |
| `database` | string | Yes | — | Database name. |
| `schema` | string | No | `mariadb` | Default schema. |
| `table` | string | Sink | — | Target table name for sink operations. |
| `filter` | string | Sink (update/upsert/delete) | — | SQL WHERE clause fragment. Use `#fieldName#` placeholders (e.g. `` `userId` = '#userId#' ``). |
| `lookup` | string | Sink | — | Alternative lookup expression. |
| `changelog` | object | Source | — | Changelog table configuration for `ReadStrategy`. |
| `commands` | map | Source | — | Named command definitions for source connectors. |

### Changelog configuration (source)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `changelog.schema` | string | No | `mariadb` | Schema of the changelog table. |
| `changelog.table` | string | Yes | — | Name of the changelog table. |
| `changelog.retention` | int | No | `1` | Days to retain processed changelog records. |

---

## Sink connector

### Strategies

| Strategy class | Description |
|----------------|-------------|
| `Kafka.Connect.MariaDb.Strategies.InsertStrategy` | Inserts each record as a new row. |
| `Kafka.Connect.MariaDb.Strategies.UpdateStrategy` | Updates rows matched by `filter`. |
| `Kafka.Connect.MariaDb.Strategies.DeleteStrategy` | Deletes rows matched by `filter`. |
| `Kafka.Connect.MariaDb.Strategies.UpsertStrategy` | Inserts or updates using `INSERT ... ON DUPLICATE KEY UPDATE`. |

### Example — upsert sink

```json
{
  "mariadb-sink-connector": {
    "groupId": "mariadb-sink-connector",
    "topics": ["product-events"],
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
        "password": "secret",
        "database": "inventory",
        "schema": "inventory",
        "table": "products",
        "filter": "`productId` = '#productId#'"
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
| `Kafka.Connect.MariaDb.Strategies.ReadStrategy` | Polls the changelog table and fetches corresponding rows. |

### Command configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | string | Yes | Kafka topic to publish records to. |
| `table` | string | Yes | Source table to read from. |
| `schema` | string | No | Schema for this command's table. Defaults to `mariadb`. |
| `keys` | string[] | Yes | Columns that uniquely identify a row. |
| `filters` | map | No | Additional equality filters. |
| `snapshot.enabled` | bool | No | When `true`, performs a full table scan first. |
| `snapshot.key` | string | No | Column used to page through the snapshot. |

### Example — source with changelog

```json
{
  "mariadb-source-connector": {
    "groupId": "mariadb-source-connector",
    "tasks": 1,
    "plugin": {
      "type": "source",
      "name": "mariadb",
      "handler": "Kafka.Connect.MariaDb.MariaDbPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.MariaDb.Strategies.ReadStrategy"
      },
      "properties": {
        "host": "localhost",
        "port": 3306,
        "userId": "root",
        "password": "secret",
        "database": "inventory",
        "changelog": {
          "schema": "inventory",
          "table": "audit_log",
          "retention": 1
        },
        "commands": {
          "read-products": {
            "topic": "product-changes",
            "table": "products",
            "schema": "inventory",
            "keys": ["productId"],
            "snapshot": {
              "enabled": false,
              "key": "productId"
            }
          }
        }
      }
    }
  }
}
```
