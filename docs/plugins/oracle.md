# Oracle Plugin

The Oracle plugin supports both **sink** and **source** connectors.

- **Sink** — writes Kafka records to an Oracle table using insert, update, delete, or upsert operations.
- **Source** — reads rows from an Oracle table using a changelog (audit) table pattern with optional initial snapshot support.

---

## Plugin registration

Add the following to `plugins.initializers` in your worker configuration:

```json
"oracle": {
  "assembly": "Kafka.Connect.Oracle.dll",
  "class": "Kafka.Connect.Oracle.DefaultPluginInitializer"
}
```

---

## Connection properties

These fields go inside `plugin.properties`:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `host` | string | Yes | — | Oracle server hostname or IP address. |
| `port` | int | No | `1521` | Oracle listener port. |
| `userId` | string | Yes | — | Database user. |
| `password` | string | Yes | — | Database password. |
| `database` | string | No | — | Database SID or service alias (used in the connection string). |
| `serviceName` | string | Yes | — | Oracle service name used to build the TNS connection string. |
| `role` | string | No | — | DBA role to connect with (e.g. `SYSDBA`). Omit for standard user connections. |
| `schema` | string | No | `SYSTEM` | Default schema for tables. |
| `table` | string | Sink | — | Target table name for sink operations. |
| `filter` | string | Sink (update/upsert/delete) | — | SQL WHERE clause fragment. Use `#fieldName#` placeholders (e.g. `"USER_ID" = '#userId#'`). |
| `lookup` | string | Sink | — | Alternative lookup expression. |
| `changelog` | object | Source | — | Changelog table configuration for `ReadStrategy`. |
| `commands` | map | Source | — | Named command definitions for source connectors. |

### Changelog configuration (source)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `changelog.schema` | string | No | `SYSTEM` | Schema of the changelog table. |
| `changelog.table` | string | Yes | — | Name of the changelog table. |
| `changelog.retention` | int | No | `1` | Days to retain processed changelog records. |

---

## Sink connector

### Strategies

| Strategy class | Description |
|----------------|-------------|
| `Kafka.Connect.Oracle.Strategies.InsertStrategy` | Inserts each record as a new row. |
| `Kafka.Connect.Oracle.Strategies.UpdateStrategy` | Updates rows matched by `filter`. |
| `Kafka.Connect.Oracle.Strategies.DeleteStrategy` | Deletes rows matched by `filter`. |
| `Kafka.Connect.Oracle.Strategies.UpsertStrategy` | Inserts or updates using `MERGE INTO` semantics. |

### Example — upsert sink

```json
{
  "oracle-sink-connector": {
    "groupId": "oracle-sink-connector",
    "topics": ["transactions"],
    "tasks": 1,
    "plugin": {
      "type": "sink",
      "name": "oracle",
      "handler": "Kafka.Connect.Oracle.OraclePluginHandler",
      "strategy": {
        "name": "Kafka.Connect.Oracle.Strategies.UpsertStrategy"
      },
      "properties": {
        "host": "localhost",
        "port": 1521,
        "userId": "myuser",
        "password": "mypassword",
        "serviceName": "ORCL",
        "schema": "MYSCHEMA",
        "table": "TRANSACTIONS",
        "filter": "\"TXN_ID\" = '#txnId#'"
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
| `Kafka.Connect.Oracle.Strategies.ReadStrategy` | Polls the changelog table and fetches corresponding rows. |

### Command configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | string | Yes | Kafka topic to publish records to. |
| `table` | string | Yes | Source table to read from. |
| `schema` | string | No | Schema for this command's table. Defaults to `SYSTEM`. |
| `keys` | string[] | Yes | Columns that uniquely identify a row. |
| `filters` | map | No | Additional equality filters. |
| `snapshot.enabled` | bool | No | When `true`, performs a full table scan first. |
| `snapshot.key` | string | No | Column used to page through the snapshot. |

### Example — source with changelog

```json
{
  "oracle-source-connector": {
    "groupId": "oracle-source-connector",
    "tasks": 1,
    "plugin": {
      "type": "source",
      "name": "oracle",
      "handler": "Kafka.Connect.Oracle.OraclePluginHandler",
      "strategy": {
        "name": "Kafka.Connect.Oracle.Strategies.ReadStrategy"
      },
      "properties": {
        "host": "localhost",
        "port": 1521,
        "userId": "myuser",
        "password": "mypassword",
        "serviceName": "ORCL",
        "schema": "MYSCHEMA",
        "changelog": {
          "schema": "MYSCHEMA",
          "table": "AUDIT_LOG",
          "retention": 1
        },
        "commands": {
          "read-transactions": {
            "topic": "transaction-changes",
            "table": "TRANSACTIONS",
            "schema": "MYSCHEMA",
            "keys": ["TXN_ID"],
            "snapshot": {
              "enabled": false,
              "key": "TXN_ID"
            }
          }
        }
      }
    }
  }
}
```
