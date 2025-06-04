# Kafka Connect .NET SQL Server Connector Configuration Documentation

This document provides a comprehensive guide to configuring SQL Server connectors in Kafka Connect .NET.

## Overview

The SQL Server connector allows you to stream data between SQL Server databases and Kafka topics. It supports both source (SQL Server to Kafka) and sink (Kafka to SQL Server) operations, with various strategies for handling data changes.

## Configuration Structure

SQL Server connector configurations are defined within the `connectors` section of the worker configuration:

```json
{
  "worker": {
    "connectors": {
      "your-sqlserver-connector-name": {
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
  "name": "default-sqlserver",
  "handler": "Kafka.Connect.SqlServer.SqlServerPluginHandler",
  "strategy": {
    "name": "Kafka.Connect.SqlServer.Strategies.ReadStrategy", // or other strategy
    "selector": "Kafka.Connect.SqlServer.Strategies.ChangelogStrategySelector"
  },
  "properties": {
    // SQL Server-specific configuration
  }
}
```

| Setting | Type | Description |
|---------|------|-------------|
| `type` | string | Connector type: `"source"` (SQL Server to Kafka) or `"sink"` (Kafka to SQL Server) |
| `name` | string | Name of the plugin initializer defined in the leader/worker configuration |
| `handler` | string | Fully qualified class name of the plugin handler |
| `strategy.name` | string | Strategy class for reading/writing data |
| `strategy.selector` | string | Strategy selector class for handling different change types |

### Common Database Connection Settings

These settings apply to both source and sink connectors:

```json
"properties": {
  "server": "localhost",
  "port": 1433,
  "userId": "sa",
  "password": "YourPassword",
  "database": "your_database",
  "integratedSecurity": false,
  "trustServerCertificate": true
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `server` | string | - | SQL Server hostname or IP address |
| `port` | integer | `1433` | SQL Server port |
| `userId` | string | - | SQL Server username |
| `password` | string | - | SQL Server password |
| `database` | string | - | SQL Server database name |
| `integratedSecurity` | boolean | `false` | Whether to use Windows Authentication |
| `trustServerCertificate` | boolean | `true` | Whether to trust the server certificate |

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

Source connectors read data from SQL Server and publish it to Kafka topics. They can operate in two modes:

1. **Direct Table Reading Mode**: Reads directly from source tables
2. **Changelog Mode**: Reads from a changelog table that tracks changes

### Source Connector Strategies

| Strategy | Description |
|----------|-------------|
| `Kafka.Connect.SqlServer.Strategies.ReadStrategy` | Reads data from SQL Server tables |

### Commands Configuration (Source Connectors)

Source connectors use the `commands` section to define specific data operations:

```json
"commands": {
  "command-name": {
    "topic": "test-customer-data",
    "table": "customer",
    "schema": "dbo",
    "keys": ["id"],
    // Additional settings based on mode
  }
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `topic` | string | - | Kafka topic to produce to |
| `table` | string | - | SQL Server table name |
| `schema` | string | `"dbo"` | Schema name for the table |
| `keys` | array | - | Primary key column(s) for the table |

### Direct Table Reading Mode

When not using changelog mode, source connectors read directly from the source tables.

#### Filters Configuration

```json
"filters": {
  "updated": 100,
  "id": 12
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
  updated > '100'
) OR (
  updated = '100' AND id > '12'
)
```

This pattern enables efficient incremental loading by:
- First fetching all records with `updated > 100`
- Then fetching records where `updated = 100` but `id > 12`

This ensures no records are missed or duplicated during incremental loads.

### Changelog Mode

When a valid changelog configuration is provided, source connectors read from the changelog table instead of directly from source tables.

#### Changelog Configuration

```json
"changelog": {
  "table": "connect_audit_logs",
  "schema": "dbo",
  "retention": 2
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `table` | string | - | Table name for storing change logs. When specified, enables changelog mode |
| `schema` | string | `"dbo"` | Schema name for the changelog table |
| `retention` | integer | `1` | Number of days to retain changelog records |

In changelog mode, the connector reads from the changelog table using a WHERE clause that filters by schema name, table name, timestamp, and log ID. This allows for efficient change data capture without directly querying the source tables.

#### Changelog Table Structure

The changelog table must have the following structure:

```sql
CREATE TABLE dbo.connect_audit_logs (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    log_schema NVARCHAR(255) NOT NULL,
    log_table NVARCHAR(255) NOT NULL,
    log_operation NVARCHAR(50) NOT NULL,
    log_timestamp DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    log_before NVARCHAR(MAX),
    log_after NVARCHAR(MAX)
);

-- Recommended index for performance
CREATE INDEX idx_connect_audit_logs_schema_table_timestamp
ON dbo.connect_audit_logs(log_schema, log_table, log_timestamp);
```

| Column | Type | Description |
|--------|------|-------------|
| `log_id` | INT IDENTITY | Unique identifier for the changelog entry |
| `log_schema` | NVARCHAR | Schema name of the source table |
| `log_table` | NVARCHAR | Table name of the source table |
| `log_operation` | NVARCHAR | Operation type (INSERT, UPDATE, DELETE) |
| `log_timestamp` | DATETIME2 | When the change occurred |
| `log_before` | NVARCHAR(MAX) | JSON representation of the record before the change (for UPDATE and DELETE) |
| `log_after` | NVARCHAR(MAX) | JSON representation of the record after the change (for INSERT and UPDATE) |

The connector uses this table to track changes to the source tables. You'll need to set up triggers on your source tables to populate this changelog table when data changes occur.

### Snapshot Configuration

The `snapshot` section configures point-in-time snapshots of the database:

```json
"snapshot": {
  "enabled": false,
  "key": "id"
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

Sink connectors consume data from Kafka topics and write it to SQL Server tables.

### Sink Connector Strategies

| Strategy | Description | Required Properties | Optional Properties |
|----------|-------------|---------------------|---------------------|
| `Kafka.Connect.SqlServer.Strategies.InsertStrategy` | Inserts new records | `table`, `schema` | - |
| `Kafka.Connect.SqlServer.Strategies.UpdateStrategy` | Updates existing records | `table`, `schema`, `lookup` | `filter` |
| `Kafka.Connect.SqlServer.Strategies.DeleteStrategy` | Deletes records | `table`, `schema`, `lookup` | `filter` |
| `Kafka.Connect.SqlServer.Strategies.UpsertStrategy` | Inserts or updates records | `table`, `schema`, `lookup` | `filter` |

### Sink Connector Properties

```json
"properties": {
  "server": "localhost",
  "port": 1433,
  "userId": "sa",
  "password": "YourPassword",
  "database": "entitlements",
  "schema": "dbo",
  "table": "customer",
  "lookup": "id={id} AND updated={updated}",
  "filter": "id={id} AND updated={updated}"
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `schema` | string | `"dbo"` | Default schema name (sink connectors only) |
| `table` | string | - | Target table name (sink connectors only) |
| `lookup` | string | Template string for looking up records. Variables in curly braces (e.g., `{id}`) are replaced with values from the incoming record |
| `filter` | string | Template string for filtering records. Similar to lookup, but used for additional filtering of which records should be processed. When provided, only records matching both the lookup and filter conditions will be processed |

Unlike source connectors that use the `commands` section to define operations, sink connectors directly specify the target `schema` and `table` at the properties level, along with `lookup` and `filter` templates for record matching.

## Processors Configuration

Processors allow you to transform data as it flows through the connector. They can be applied to both source and sink connectors to modify records before they are written to Kafka or the database.

```json
"processors": {
  "1": {
    "name": "Kafka.Connect.Processors.FieldRenamer",
    "settings": {
      "first_name": "firstName",
      "last_name": "lastName"
    }
  },
  "2": {
    "name": "Kafka.Connect.Processors.DateTimeTypeOverrider",
    "settings": {
      "created_at": "yyyy-MM-dd'T'HH:mm:ss.fff'Z'"
    }
  }
}
```

The processors are executed in the order specified by the numeric keys.

### Available Processors

| Processor | Description | Settings Type | Example |
|-----------|-------------|--------------|---------|
| `Kafka.Connect.Processors.FieldRenamer` | Renames fields in the record | Dictionary<string, string> | `{"old_name": "newName"}` |
| `Kafka.Connect.Processors.BlacklistFieldProjector` | Removes specified fields from the record | List<string> | `["sensitive_field", "internal_id"]` |
| `Kafka.Connect.Processors.WhitelistFieldProjector` | Keeps only specified fields in the record | List<string> | `["id", "name", "email"]` |
| `Kafka.Connect.Processors.DateTimeTypeOverrider` | Converts string fields to DateTime objects | Dictionary<string, string> | `{"created_at": "yyyy-MM-dd'T'HH:mm:ss"}` |
| `Kafka.Connect.Processors.JsonTypeOverrider` | Parses JSON strings into structured objects | List<string> | `["metadata", "attributes"]` |

### Field Patterns

Processors support pattern matching for field names:

- Use `*` as a wildcard to match multiple fields (e.g., `user.*` matches all fields starting with "user.")
- Prefix with `key.` to apply to the record key instead of the value (e.g., `key.id`)
- Use brackets `[]` for special characters in field names (e.g., `[user.first-name]`)

### Example Processor Configurations

#### Field Renaming

```json
"processors": {
  "1": {
    "name": "Kafka.Connect.Processors.FieldRenamer",
    "settings": {
      "user_id": "userId",
      "created_at": "createdAt",
      "user.*": "profile.*"
    }
  }
}
```

#### Date/Time Conversion

```json
"processors": {
  "1": {
    "name": "Kafka.Connect.Processors.DateTimeTypeOverrider",
    "settings": {
      "created_at": "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
      "updated_at": "",  // Empty string uses default parsing
      "*.date": "yyyy-MM-dd"  // Apply to all fields ending with .date
    }
  }
}
```

#### Field Filtering

```json
"processors": {
  "1": {
    "name": "Kafka.Connect.Processors.BlacklistFieldProjector",
    "settings": ["password", "ssn", "credit_card", "*.sensitive"]
  }
}
```

#### JSON Parsing

```json
"processors": {
  "1": {
    "name": "Kafka.Connect.Processors.JsonTypeOverrider",
    "settings": ["metadata", "attributes", "user_preferences"]
  }
}
```

## Example Configurations

### Source Connector Example

```json
{
  "worker": {
    "connectors": {
      "source-connector-sqlserver": {
        "groupId": "customer-data-connector",
        "tasks": 1,
        "paused": false,
        "disabled": false,
        "plugin": {
          "type": "source",
          "name": "default-sqlserver",
          "handler": "Kafka.Connect.SqlServer.SqlServerPluginHandler",
          "strategy": {
            "name": "Kafka.Connect.SqlServer.Strategies.ReadStrategy",
            "selector": "Kafka.Connect.SqlServer.Strategies.ChangelogStrategySelector"
          },
          "properties": {
            "server": "sqlserver.example.com",
            "port": 1433,
            "userId": "app_user",
            "password": "secure_password",
            "database": "customer_db",
            "integratedSecurity": false,
            "trustServerCertificate": true,
            "changelog": {
              "table": "connect_audit_logs",
              "retention": 7
            },
            "commands": {
              "read-customers": {
                "topic": "customer-data",
                "table": "customers",
                "schema": "dbo",
                "keys": ["customer_id"],
                "snapshot": {
                  "enabled": true,
                  "key": "customer_id"
                }
              },
              "read-orders": {
                "topic": "order-data",
                "table": "orders",
                "schema": "dbo",
                "keys": ["order_id"],
                "filters": {
                  "status": "ACTIVE"
                }
              }
            }
          }
        },
        "processors": {
          "1": {
            "name": "Kafka.Connect.Processors.FieldRenamer",
            "settings": {
              "customer_id": "customerId",
              "created_at": "createdAt"
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
      "sink-connector-sqlserver": {
        "groupId": "customer-data-sink-connector",
        "topics": ["test-customer-data"],
        "tasks": 1,
        "paused": false,
        "disabled": false,
        "plugin": {
          "type": "sink",
          "name": "default-sqlserver",
          "handler": "Kafka.Connect.SqlServer.SqlServerPluginHandler",
          "strategy": {
            "name": "Kafka.Connect.SqlServer.Strategies.UpsertStrategy",
            "selector": "Kafka.Connect.SqlServer.Strategies.ChangelogStrategySelector"
          },
          "properties": {
            "server": "localhost",
            "port": 1433,
            "userId": "sa",
            "password": "YourPassword",
            "database": "entitlements",
            "integratedSecurity": false,
            "trustServerCertificate": true,
            "schema": "dbo",
            "table": "customer",
            "lookup": "id={id} AND updated={updated}",
            "filter": "id={id} AND updated={updated}"
          }
        },
        "processors": {
          "1": {
            "name": "Kafka.Connect.Processors.BlacklistFieldProjector",
            "settings": ["password", "ssn", "credit_card"]
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