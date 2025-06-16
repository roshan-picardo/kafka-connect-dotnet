# Kafka Connect .NET Oracle Connector Configuration Documentation

This document provides a comprehensive guide to configuring Oracle connectors in Kafka Connect .NET.

## Overview

The Oracle connector allows you to stream data between Oracle databases and Kafka topics. It supports both source (Oracle to Kafka) and sink (Kafka to Oracle) operations, with various strategies for handling data changes.

## Configuration Structure

Oracle connector configurations are defined within the `connectors` section of the worker configuration:

```json
{
  "worker": {
    "connectors": {
      "your-oracle-connector-name": {
        // Connector configuration
      }
    }
  }
}
```

## Common Configuration

### Basic Connector Settings

These settings apply to both source and sink connectors:

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `groupId` | string | - | Consumer group ID for the connector |
| `tasks` | integer | `1` | Number of tasks to create for this connector |
| `paused` | boolean | `false` | Whether the connector should start in paused state |
| `disabled` | boolean | `false` | Whether the connector is disabled |
| `topics` | array | `[]` | List of topics to consume from (for sink connectors only) |

### Plugin Configuration

The `plugin` section defines the connector type and behavior:

```json
"plugin": {
  "type": "source", // or "sink"
  "name": "default-oracle",
  "handler": "Kafka.Connect.Oracle.OraclePluginHandler",
  "strategy": {
    "name": "Kafka.Connect.Oracle.Strategies.ReadStrategy", // or other strategy
    "selector": "Kafka.Connect.Oracle.Strategies.ChangelogStrategySelector"
  },
  "properties": {
    // Oracle-specific configuration
  }
}
```

| Setting | Type | Description |
|---------|------|-------------|
| `type` | string | Connector type: `"source"` (Oracle to Kafka) or `"sink"` (Kafka to Oracle) |
| `name` | string | Name of the plugin initializer defined in the leader/worker configuration |
| `handler` | string | Fully qualified class name of the plugin handler |
| `strategy.name` | string | Strategy class for reading/writing data |
| `strategy.selector` | string | Strategy selector class for handling different change types |

### Common Database Connection Settings

These settings apply to both source and sink connectors:

```json
"properties": {
  "host": "localhost",
  "port": 1521,
  "userId": "kafka_connect",
  "password": "kafka_connect",
  "database": "XEPDB1",
  "serviceName": "XEPDB1"
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `host` | string | - | Oracle hostname or IP address |
| `port` | integer | `1521` | Oracle listener port |
| `userId` | string | - | Oracle username |
| `password` | string | - | Oracle password |
| `database` | string | - | Oracle database name |
| `serviceName` | string | - | Oracle service name |

### Logging Configuration

The `log` section configures what information is logged:

```json
"log": {
  "provider": "",
  "attributes": [
    "timestamp",
    "operation",
    "after.id",
    "after.name"
  ]
}
```

| Setting | Type | Description |
|---------|------|-------------|
| `provider` | string | Custom log provider (leave empty for default) |
| `attributes` | array | List of attributes to include in logs |

## Source Connector Configuration

Source connectors read data from Oracle and publish it to Kafka topics. They can operate in two modes:

1. **Direct Table Reading Mode**: Reads directly from source tables
2. **Changelog Mode**: Reads from a changelog table that tracks changes

### Source Connector Strategies

| Strategy | Description |
|----------|-------------|
| `Kafka.Connect.Oracle.Strategies.ReadStrategy` | Reads data from Oracle tables |

### Commands Configuration (Source Connectors)

Source connectors use the `commands` section to define specific data operations:

```json
"commands": {
  "command-name": {
    "topic": "test-customer-data",
    "table": "CUSTOMERS",
    "schema": "KAFKA_CONNECT",
    "keys": ["CUSTOMER_ID"],
    // Additional settings based on mode
  }
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `topic` | string | - | Kafka topic to produce to |
| `table` | string | - | Oracle table name |
| `schema` | string | `"SYSTEM"` | Schema name for the table |
| `keys` | array | - | Primary key column(s) for the table |

### Direct Table Reading Mode

When not using changelog mode, source connectors read directly from the source tables.

#### Filters Configuration

```json
"filters": {
  "UPDATED": 100,
  "ID": 12
}
```

| Setting | Type | Description |
|---------|------|-------------|
| `filters` | object | Key-value pairs for filtering records (column name and value) |

#### How WHERE Clauses Are Built

The connector builds WHERE clauses dynamically based on the filter configuration. For incremental loading, it uses a sophisticated approach:

With the filters above, the connector builds a WHERE clause that:

1. Creates conditions for each filter combination
2. Uses equality (`=`) for previous columns and greater-than (`>`) for the current column
3. Combines conditions with OR operators

For example:

```sql
WHERE (
  "UPDATED" > '100'
) OR (
  "UPDATED" = '100' AND "ID" > '12'
)
```

This pattern enables efficient incremental loading by:
- First fetching all records with `UPDATED > 100`
- Then fetching records where `UPDATED = 100` but `ID > 12`

This ensures no records are missed or duplicated during incremental loads.

### Changelog Mode

When a valid changelog configuration is provided, source connectors read from the changelog table instead of directly from source tables.

#### Changelog Configuration

```json
"changelog": {
  "table": "CONNECT_AUDIT_LOGS",
  "schema": "KAFKA_CONNECT",
  "retention": 2
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `table` | string | - | Table name for storing change logs. When specified, enables changelog mode |
| `schema` | string | `"SYSTEM"` | Schema name for the changelog table |
| `retention` | integer | `1` | Number of days to retain changelog records |

In changelog mode, the connector reads from the changelog table using a WHERE clause that filters by schema name, table name, timestamp, and log ID. This allows for efficient change data capture without directly querying the source tables.

#### Changelog Table Structure

The changelog table must have the following structure:

```sql
CREATE TABLE "KAFKA_CONNECT"."CONNECT_AUDIT_LOGS" (
    LOG_ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    LOG_SCHEMA VARCHAR2(255) NOT NULL,
    LOG_TABLE VARCHAR2(255) NOT NULL,
    LOG_OPERATION VARCHAR2(50) NOT NULL,
    LOG_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    LOG_BEFORE CLOB,
    LOG_AFTER CLOB
);

-- Recommended index for performance
CREATE INDEX IDX_CONNECT_AUDIT_LOGS_SCHEMA_TABLE_TS
ON "KAFKA_CONNECT"."CONNECT_AUDIT_LOGS"(LOG_SCHEMA, LOG_TABLE, LOG_TIMESTAMP);
```

| Column | Type | Description |
|--------|------|-------------|
| `LOG_ID` | NUMBER GENERATED ALWAYS AS IDENTITY | Unique identifier for the changelog entry |
| `LOG_SCHEMA` | VARCHAR2 | Schema name of the source table |
| `LOG_TABLE` | VARCHAR2 | Table name of the source table |
| `LOG_OPERATION` | VARCHAR2 | Operation type (INSERT, UPDATE, DELETE) |
| `LOG_TIMESTAMP` | TIMESTAMP | When the change occurred |
| `LOG_BEFORE` | CLOB | JSON representation of the record before the change (for UPDATE and DELETE) |
| `LOG_AFTER` | CLOB | JSON representation of the record after the change (for INSERT and UPDATE) |

The connector uses this table to track changes to the source tables. You'll need to set up triggers on your source tables to populate this changelog table when data changes occur.

### Snapshot Configuration

The `snapshot` section configures point-in-time snapshots of the database:

```json
"snapshot": {
  "enabled": false,
  "key": "CUSTOMER_ID"
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `enabled` | boolean | `false` | Whether to enable snapshot mode |
| `key` | string | - | Column name to use for tracking snapshot progress |
| `id` | long | `0` | Current snapshot ID |
| `total` | long | `0` | Total number of records in the snapshot |
| `timestamp` | long | `0` | Timestamp of the snapshot |

## Sink Connector Configuration

Sink connectors consume data from Kafka topics and write it to Oracle tables.

### Sink Connector Strategies

| Strategy | Description | Required Properties | Optional Properties |
|----------|-------------|---------------------|---------------------|
| `Kafka.Connect.Oracle.Strategies.InsertStrategy` | Inserts new records | `table`, `schema` | - |
| `Kafka.Connect.Oracle.Strategies.UpdateStrategy` | Updates existing records | `table`, `schema`, `lookup` | `filter` |
| `Kafka.Connect.Oracle.Strategies.DeleteStrategy` | Deletes records | `table`, `schema`, `lookup` | `filter` |
| `Kafka.Connect.Oracle.Strategies.UpsertStrategy` | Inserts or updates records | `table`, `schema`, `lookup` | `filter` |

### Sink Connector Properties

```json
"properties": {
  "host": "localhost",
  "port": 1521,
  "userId": "kafka_connect",
  "password": "kafka_connect",
  "database": "XEPDB1",
  "serviceName": "XEPDB1",
  "schema": "KAFKA_CONNECT",
  "table": "CUSTOMERS",
  "lookup": "ID={id} AND UPDATED={updated}",
  "filter": "ID={id} AND UPDATED={updated}"
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `schema` | string | `"SYSTEM"` | Default schema name (sink connectors only) |
| `table` | string | - | Target table name (sink connectors only) |
| `lookup` | string | - | Template string for looking up records. Variables in curly braces (e.g., `{id}`) are replaced with values from the incoming record |
| `filter` | string | - | Template string for filtering records. Similar to lookup, but used for additional filtering of which records should be processed. When provided, only records matching both the lookup and filter conditions will be processed |

Unlike source connectors that use the `commands` section to define operations, sink connectors directly specify the target `schema` and `table` at the properties level, along with `lookup` and `filter` templates for record matching.

## Processors Configuration

Processors allow you to transform data as it flows through the connector. They can be applied to both source and sink connectors to modify records before they are written to Kafka or the database.

```json
"processors": {
  "1": {
    "name": "Kafka.Connect.Processors.FieldRenamer",
    "settings": {
      "FIRST_NAME": "firstName",
      "LAST_NAME": "lastName"
    }
  },
  "2": {
    "name": "Kafka.Connect.Processors.DateTimeTypeOverrider",
    "settings": {
      "CREATED_AT": "yyyy-MM-dd'T'HH:mm:ss.fff'Z'"
    }
  }
}
```

The processors are executed in the order specified by the numeric keys.

### Available Processors

| Processor | Description | Settings Type | Example |
|-----------|-------------|--------------|---------|
| `Kafka.Connect.Processors.FieldRenamer` | Renames fields in the record | Dictionary<string, string> | `{"OLD_NAME": "newName"}` |
| `Kafka.Connect.Processors.BlacklistFieldProjector` | Removes specified fields from the record | List<string> | `["SENSITIVE_FIELD", "INTERNAL_ID"]` |
| `Kafka.Connect.Processors.WhitelistFieldProjector` | Keeps only specified fields in the record | List<string> | `["ID", "NAME", "EMAIL"]` |
| `Kafka.Connect.Processors.DateTimeTypeOverrider` | Converts string fields to DateTime objects | Dictionary<string, string> | `{"CREATED_AT": "yyyy-MM-dd'T'HH:mm:ss"}` |
| `Kafka.Connect.Processors.JsonTypeOverrider` | Parses JSON strings into structured objects | List<string> | `["METADATA", "ATTRIBUTES"]` |

### Field Patterns

Processors support pattern matching for field names:

- Use `*` as a wildcard to match multiple fields (e.g., `USER.*` matches all fields starting with "USER.")
- Prefix with `key.` to apply to the record key instead of the value (e.g., `key.ID`)
- Use brackets `[]` for special characters in field names (e.g., `[USER.FIRST-NAME]`)

## Example Configurations

### Source Connector Example

```json
{
  "worker": {
    "connectors": {
      "source-connector-oracle": {
        "groupId": "customer-data-connector",
        "tasks": 1,
        "paused": false,
        "disabled": false,
        "plugin": {
          "type": "source",
          "name": "default-oracle",
          "handler": "Kafka.Connect.Oracle.OraclePluginHandler",
          "strategy": {
            "name": "Kafka.Connect.Oracle.Strategies.ReadStrategy",
            "selector": "Kafka.Connect.Oracle.Strategies.ChangelogStrategySelector"
          },
          "properties": {
            "host": "localhost",
            "port": 1521,
            "userId": "kafka_connect",
            "password": "kafka_connect",
            "database": "XEPDB1",
            "serviceName": "XEPDB1",
            "changelog": {
              "table": "CONNECT_AUDIT_LOGS",
              "schema": "KAFKA_CONNECT",
              "retention": 7
            },
            "commands": {
              "read-customers": {
                "topic": "customer-data",
                "table": "CUSTOMERS",
                "schema": "KAFKA_CONNECT",
                "keys": ["CUSTOMER_ID"],
                "snapshot": {
                  "enabled": true,
                  "key": "CUSTOMER_ID"
                }
              },
              "read-orders": {
                "topic": "order-data",
                "table": "ORDERS",
                "schema": "KAFKA_CONNECT",
                "keys": ["ORDER_ID"],
                "filters": {
                  "STATUS": "ACTIVE"
                }
              }
            }
          }
        },
        "processors": {
          "1": {
            "name": "Kafka.Connect.Processors.FieldRenamer",
            "settings": {
              "CUSTOMER_ID": "customerId",
              "CREATED_AT": "createdAt"
            }
          },
          "2": {
            "name": "Kafka.Connect.Processors.DateTimeTypeOverrider",
            "settings": {
              "createdAt": "yyyy-MM-dd'T'HH:mm:ss"
            }
          }
        },
        "log": {
          "attributes": [
            "timestamp",
            "operation",
            "after.customerId",
            "after.name",
            "after.email"
          ]
        }
      }
    }
  }
}
```

### Sink Connector Example

```json
{
  "worker": {
    "connectors": {
      "sink-connector-oracle": {
        "groupId": "customer-data-sink-connector",
        "topics": ["test-customer-data"],
        "tasks": 1,
        "paused": false,
        "disabled": false,
        "plugin": {
          "type": "sink",
          "name": "default-oracle",
          "handler": "Kafka.Connect.Oracle.OraclePluginHandler",
          "strategy": {
            "name": "Kafka.Connect.Oracle.Strategies.UpsertStrategy",
            "selector": "Kafka.Connect.Oracle.Strategies.ChangelogStrategySelector"
          },
          "properties": {
            "host": "localhost",
            "port": 1521,
            "userId": "kafka_connect",
            "password": "kafka_connect",
            "database": "XEPDB1",
            "serviceName": "XEPDB1",
            "schema": "KAFKA_CONNECT",
            "table": "CUSTOMERS",
            "lookup": "ID={id} AND UPDATED={updated}",
            "filter": "ID={id} AND UPDATED={updated}"
          }
        },
        "processors": {
          "1": {
            "name": "Kafka.Connect.Processors.BlacklistFieldProjector",
            "settings": ["PASSWORD", "SSN", "CREDIT_CARD"]
          }
        },
        "log": {
          "attributes": [
            "timestamp",
            "operation",
            "after.id",
            "after.name"
          ]
        }
      }
    }
  }
}
```

## Oracle-Specific Considerations

### Case Sensitivity

Oracle identifiers (table names, column names) are case-sensitive when quoted with double quotes. By default, Oracle stores identifiers in uppercase. The connector uses double quotes around identifiers to ensure proper case handling.

### JSON Functions

Oracle uses `JSON_OBJECT` function for JSON serialization, which is different from PostgreSQL's `ROW_TO_JSON` function. The connector handles this difference internally.

### Timestamp Handling

Oracle's timestamp handling differs from other databases. The connector uses Oracle-specific timestamp extraction functions to ensure proper timestamp conversion.

### Triggers for Change Data Capture

For changelog mode, you'll need to create triggers on your source tables. Here's an example trigger:

```sql
CREATE OR REPLACE TRIGGER "KAFKA_CONNECT"."TRG_CUSTOMERS_AUDIT_LOG"
AFTER INSERT OR UPDATE OR DELETE ON "KAFKA_CONNECT"."CUSTOMERS"
FOR EACH ROW
DECLARE
    v_operation VARCHAR2(10);
    v_before CLOB;
    v_after CLOB;
BEGIN
    IF INSERTING THEN
        v_operation := 'INSERT';
        v_before := NULL;
        v_after := json_object(
            'CUSTOMER_ID' VALUE :NEW.CUSTOMER_ID,
            'NAME' VALUE :NEW.NAME,
            'EMAIL' VALUE :NEW.EMAIL,
            'CREATED_AT' VALUE :NEW.CREATED_AT
        );
    ELSIF UPDATING THEN
        v_operation := 'UPDATE';
        v_before := json_object(
            'CUSTOMER_ID' VALUE :OLD.CUSTOMER_ID,
            'NAME' VALUE :OLD.NAME,
            'EMAIL' VALUE :OLD.EMAIL,
            'CREATED_AT' VALUE :OLD.CREATED_AT
        );
        v_after := json_object(
            'CUSTOMER_ID' VALUE :NEW.CUSTOMER_ID,
            'NAME' VALUE :NEW.NAME,
            'EMAIL' VALUE :NEW.EMAIL,
            'CREATED_AT' VALUE :NEW.CREATED_AT
        );
    ELSIF DELETING THEN
        v_operation := 'DELETE';
        v_before := json_object(
            'CUSTOMER_ID' VALUE :OLD.CUSTOMER_ID,
            'NAME' VALUE :OLD.NAME,
            'EMAIL' VALUE :OLD.EMAIL,
            'CREATED_AT' VALUE :OLD.CREATED_AT
        );
        v_after := NULL;
    END IF;
    
    INSERT INTO "KAFKA_CONNECT"."CONNECT_AUDIT_LOGS"
        (LOG_SCHEMA, LOG_TABLE, LOG_OPERATION, LOG_BEFORE, LOG_AFTER)
    VALUES
        ('KAFKA_CONNECT', 'CUSTOMERS', v_operation, v_before, v_after);
END;
/
```

## Best Practices

### Common Best Practices

1. **Security**: Use a dedicated database user with minimal required permissions
2. **Connection Pooling**: For high-throughput scenarios, consider configuring connection pooling
3. **Monitoring**: Enable appropriate logging attributes to monitor connector performance
4. **Keys**: Always specify primary keys correctly to ensure proper record identification

### Source Connector Best Practices

1. **Changelog Mode**: Use changelog mode for production environments to capture all data changes
2. **Snapshots**: Use snapshots for initial data loading, then switch to change data capture
3. **Filters**: Use filters to limit the data processed when in direct table reading mode
4. **Changelog Retention**: Configure appropriate retention periods based on your recovery needs

### Sink Connector Best Practices

1. **Lookup Templates**: Design lookup templates that uniquely identify records
2. **Strategy Selection**: Choose the appropriate strategy based on your needs:
   - Use `InsertStrategy` for append-only operations
   - Use `UpdateStrategy` when records always exist and need updating
   - Use `DeleteStrategy` for removing records
   - Use `UpsertStrategy` (most common) when you need to insert or update based on existence
3. **Filter Templates**: Use filter templates to limit which records are processed