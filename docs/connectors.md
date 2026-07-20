# Connectors

A connector defines a data pipeline between Kafka and an external system. Every connector is either a **source** (reads data from an external system and writes it to Kafka) or a **sink** (reads data from Kafka and writes it to an external system).

---

## Source Connectors

A source connector continuously reads data from an external system and publishes records to one or more Kafka topics.

```
External System  →  Source Connector  →  Kafka Topic
(database, stream, etc.)
```

### How source connectors work

1. The worker starts a `SourceTask` for each configured task slot.
2. The task calls the plugin's strategy to fetch records from the external system.
3. Records are wrapped as Kafka messages and published to the configured topic.
4. The task tracks a cursor (timestamp, sequence number, snapshot ID) so it can resume from the last position after a restart.

### Source strategies

| Strategy | Description |
|----------|-------------|
| `ReadStrategy` | Polls an external table or collection using a changelog or cursor table. Suitable for relational databases with an audit/changelog table. |
| `StreamsReadStrategy` | Watches a MongoDB change stream. Receives change events in real time without polling. |
| `StreamReadStrategy` | Reads from DynamoDB Streams to capture item-level changes. |

### Snapshot support

Most source plugins support an initial **snapshot** — a full read of the source table before switching to incremental polling. Configure it per command:

```json
"snapshot": {
  "enabled": true,
  "key": "userId"
}
```

When `enabled` is `true` and the snapshot has not been completed (`total == 0`), the connector performs a full table scan ordered by `key`. Once the snapshot is complete it transitions to incremental polling using the changelog/cursor.

### Connector configuration fields (source)

These fields apply inside the connector definition:

| Field | Type | Description |
|-------|------|-------------|
| `groupId` | string | Kafka consumer group ID for this connector. Defaults to the connector name. |
| `tasks` | int | Number of parallel tasks to run for this connector. |
| `paused` | bool | When `true`, the connector is registered but not started. |
| `disabled` | bool | When `true`, the connector is completely ignored. |
| `plugin.type` | string | Must be `source` for source connectors. |
| `plugin.name` | string | Plugin prefix that matches an entry in `plugins.initializers`. |
| `plugin.handler` | string | Fully-qualified class name of the plugin handler. |
| `plugin.strategy.name` | string | Fully-qualified class name of the strategy to use for reading. |
| `plugin.properties` | object | Plugin-specific configuration. See individual plugin docs. |
| `processors` | map | Optional ordered chain of record processors. See [Processors](#processors). |
| `log.provider` | string | Fully-qualified class name of the log record provider. |
| `log.attributes` | string[] | Fields to include in structured log output for this connector. |

---

## Sink Connectors

A sink connector reads records from one or more Kafka topics and writes them to an external system.

```
Kafka Topic  →  Sink Connector  →  External System
```

### How sink connectors work

1. The worker starts a `SinkTask` for each configured task slot.
2. The task polls its assigned Kafka topics for new records.
3. Records are batched and passed through any configured processors.
4. The plugin's strategy executes the configured write operation (insert, update, upsert, or delete) against the external system.
5. Kafka offsets are committed after a successful batch write.

### Sink strategies

| Strategy | Description |
|----------|-------------|
| `InsertStrategy` | Inserts every record as a new row/document. Does not check for existing records. |
| `UpdateStrategy` | Updates existing records matched by a filter expression. Records that do not match are skipped. |
| `DeleteStrategy` | Deletes records from the target matched by a filter expression. |
| `UpsertStrategy` | Inserts the record if it does not exist, or updates it if it does. The most common strategy for sync pipelines. |

### Filter and lookup expressions

Sink strategies that need to identify the target record use `filter` and `lookup` expressions. These are template strings where `#fieldName#` is replaced at runtime with the corresponding field value from the Kafka record.

```json
"filter": "{\"userId\": \"#userId#\"}"          // MongoDB JSON filter
"lookup": "`userId` = '#userId#'"               // SQL WHERE clause
```

### Connector configuration fields (sink)

| Field | Type | Description |
|-------|------|-------------|
| `groupId` | string | Kafka consumer group ID. Defaults to the connector name. |
| `topics` | string[] | List of Kafka topics to consume. |
| `tasks` | int | Number of parallel tasks. |
| `paused` | bool | Registered but not running when `true`. |
| `disabled` | bool | Completely excluded when `true`. |
| `converters` | object | Per-connector converter override (key/value). Overrides the worker-level converters. |
| `faultTolerance` | object | Per-connector fault tolerance override. Overrides the worker-level fault tolerance. |
| `plugin.type` | string | Must be `sink` for sink connectors. |
| `plugin.name` | string | Plugin prefix matching `plugins.initializers`. |
| `plugin.handler` | string | Fully-qualified class name of the plugin handler. |
| `plugin.strategy.name` | string | Fully-qualified class name of the strategy to use for writing. |
| `plugin.properties` | object | Plugin-specific configuration. See individual plugin docs. |
| `processors` | map | Optional ordered processor chain. |
| `log.provider` | string | Log record provider class. |
| `log.attributes` | string[] | Fields to include in structured log output. |

---

## Processors

Processors transform records as they flow through the connector. They can be used on both source and sink connectors. Multiple processors can be chained by using ordered numeric keys (`"1"`, `"2"`, etc.).

```json
"processors": {
  "1": {
    "name": "Kafka.Connect.Processors.BlacklistFieldProjector",
    "settings": ["internalId", "auditUser"]
  },
  "2": {
    "name": "Kafka.Connect.Processors.FieldRenamer",
    "settings": {
      "user_id": "userId",
      "created_at": "createdAt"
    }
  }
}
```

### Available processors

#### WhitelistFieldProjector

Keeps only the listed fields and removes everything else.

```json
{
  "name": "Kafka.Connect.Processors.WhitelistFieldProjector",
  "settings": ["userId", "userName", "email"]
}
```

`settings` is an array of field names to retain.

#### BlacklistFieldProjector

Removes the listed fields and keeps everything else.

```json
{
  "name": "Kafka.Connect.Processors.BlacklistFieldProjector",
  "settings": ["password", "ssn", "creditCard"]
}
```

`settings` is an array of field names to drop.

#### FieldRenamer

Renames fields. The map key is the original field name; the value is the new name.

```json
{
  "name": "Kafka.Connect.Processors.FieldRenamer",
  "settings": {
    "user_id": "userId",
    "created_at": "createdAt"
  }
}
```

#### DateTimeTypeOverrider

Converts date/time fields to a specific string format.

```json
{
  "name": "Kafka.Connect.Processors.DateTimeTypeOverrider",
  "settings": {
    "createdAt": "yyyy-MM-dd HH:mm:ss",
    "eventDate": "yyyy-MM-dd"
  }
}
```

The map key is the field name; the value is the .NET format string.

#### JsonTypeOverrider

Coerces field values to a specific JSON type.

```json
{
  "name": "Kafka.Connect.Processors.JsonTypeOverrider",
  "settings": {
    "age": "int",
    "price": "decimal",
    "isActive": "bool"
  }
}
```

The map key is the field name; the value is the target type (`int`, `decimal`, `bool`, `string`, etc.).

---

## Topic-Level Overrides

Individual topics within a connector can have their own converter and processor configuration via the `overrides` map. This is useful when a connector consumes multiple topics with different message formats.

```json
{
  "overrides": {
    "events-topic": {
      "converters": {
        "key": "Kafka.Connect.Converters.StringConverter",
        "value": "Kafka.Connect.Converters.AvroConverter"
      },
      "processors": {
        "1": {
          "name": "Kafka.Connect.Processors.FieldRenamer",
          "settings": { "event_type": "eventType" }
        }
      }
    }
  }
}
```
