# SQL Server Plugin

The SQL Server plugin supports both **sink** and **source** connectors.

- **Sink** — writes Kafka records to a SQL Server table using insert, update, delete, or upsert operations.
- **Source** — reads rows from a SQL Server table using a changelog (audit) table pattern with optional initial snapshot support.

---

## Plugin registration

Add the following to `plugins.initializers` in your worker configuration:

```json
"sqlserver": {
  "assembly": "Kafka.Connect.SqlServer.dll",
  "class": "Kafka.Connect.SqlServer.DefaultPluginInitializer"
}
```

---

## Connection properties

These fields go inside `plugin.properties`:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `server` | string | Yes | — | SQL Server hostname or IP address. |
| `port` | int | No | `1433` | SQL Server port. |
| `userId` | string | Yes* | — | Database user. Not required when `integratedSecurity` is `true`. |
| `password` | string | Yes* | — | Database password. Not required when `integratedSecurity` is `true`. |
| `database` | string | Yes | — | Database name. |
| `integratedSecurity` | bool | No | `false` | Use Windows Integrated Security instead of SQL login. |
| `trustServerCertificate` | bool | No | `true` | Skip SSL certificate validation. Set to `false` in production with a valid certificate. |
| `schema` | string | No | `dbo` | Default schema for tables. |
| `table` | string | Sink | — | Target table name for sink operations. |
| `filter` | string | Sink (update/upsert/delete) | — | SQL WHERE clause fragment. Use `#fieldName#` placeholders (e.g. `[userId] = '#userId#'`). |
| `lookup` | string | Sink | — | Alternative lookup expression for strategies that distinguish lookup from filter. |
| `changelog` | object | Source | — | Changelog table configuration for `ReadStrategy`. |
| `commands` | map | Source | — | Named command definitions for source connectors. |

### Changelog configuration (source)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `changelog.schema` | string | No | `dbo` | Schema of the changelog table. |
| `changelog.table` | string | Yes | — | Name of the changelog table. |
| `changelog.retention` | int | No | `1` | Days to retain processed changelog records. |

---

## Sink connector

### Strategies

| Strategy class | Description |
|----------------|-------------|
| `Kafka.Connect.SqlServer.Strategies.InsertStrategy` | Inserts each record as a new row. |
| `Kafka.Connect.SqlServer.Strategies.UpdateStrategy` | Updates rows matched by `filter`. |
| `Kafka.Connect.SqlServer.Strategies.DeleteStrategy` | Deletes rows matched by `filter`. |
| `Kafka.Connect.SqlServer.Strategies.UpsertStrategy` | Inserts or updates using `MERGE` semantics. |

### Example — upsert sink

```json
{
  "sqlserver-sink-connector": {
    "groupId": "sqlserver-sink-connector",
    "topics": ["orders"],
    "tasks": 1,
    "plugin": {
      "type": "sink",
      "name": "sqlserver",
      "handler": "Kafka.Connect.SqlServer.SqlServerPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.SqlServer.Strategies.UpsertStrategy"
      },
      "properties": {
        "server": "localhost",
        "port": 1433,
        "userId": "sa",
        "password": "YourPassword",
        "database": "OrdersDB",
        "schema": "dbo",
        "table": "Orders",
        "filter": "[orderId] = '#orderId#'"
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
| `Kafka.Connect.SqlServer.Strategies.ReadStrategy` | Polls the changelog table and fetches corresponding rows. |

### Command configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | string | Yes | Kafka topic to publish records to. |
| `table` | string | Yes | Source table to read from. |
| `schema` | string | No | Schema for this command's table. Defaults to `dbo`. |
| `keys` | string[] | Yes | Columns that uniquely identify a row. |
| `filters` | map | No | Additional equality filters. |
| `snapshot.enabled` | bool | No | When `true`, performs a full table scan first. |
| `snapshot.key` | string | No | Column used to page through the snapshot. |

### Example — source with changelog

```json
{
  "sqlserver-source-connector": {
    "groupId": "sqlserver-source-connector",
    "tasks": 1,
    "plugin": {
      "type": "source",
      "name": "sqlserver",
      "handler": "Kafka.Connect.SqlServer.SqlServerPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.SqlServer.Strategies.ReadStrategy"
      },
      "properties": {
        "server": "localhost",
        "port": 1433,
        "userId": "sa",
        "password": "YourPassword",
        "database": "OrdersDB",
        "changelog": {
          "schema": "dbo",
          "table": "AuditLog",
          "retention": 1
        },
        "commands": {
          "read-orders": {
            "topic": "order-changes",
            "table": "Orders",
            "schema": "dbo",
            "keys": ["orderId"],
            "snapshot": {
              "enabled": false,
              "key": "orderId"
            }
          }
        }
      }
    }
  }
}
```
