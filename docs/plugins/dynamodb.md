# DynamoDB Plugin

The DynamoDB plugin supports both **sink** and **source** connectors.

- **Sink** — writes Kafka records to a DynamoDB table using insert, update, delete, or upsert operations.
- **Source** — reads items from DynamoDB using either a scan/query approach (`ReadStrategy`) or DynamoDB Streams (`StreamReadStrategy`).

---

## Plugin registration

Add the following to `plugins.initializers` in your worker configuration:

```json
"dynamodb": {
  "assembly": "Kafka.Connect.DynamoDb.dll",
  "class": "Kafka.Connect.DynamoDb.DefaultPluginInitializer"
}
```

---

## Connection properties

These fields go inside `plugin.properties`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `region` | string | Yes* | AWS region (e.g. `us-east-1`). Required unless `serviceUrl` is set. |
| `accessKeyId` | string | Yes* | AWS access key ID. Not required when using IAM roles or instance profiles. |
| `secretAccessKey` | string | Yes* | AWS secret access key. Not required when using IAM roles or instance profiles. |
| `serviceUrl` | string | No | Custom endpoint URL. Use this to point to a local DynamoDB emulator (e.g. `http://localhost:8000`). When set, `region` is not required. |
| `tableName` | string | Sink | Target DynamoDB table name for sink operations. |
| `isWriteOrdered` | bool | No | `true` | Whether bulk write operations maintain order. |
| `filter` | string | Sink (update/upsert/delete) | Condition expression to identify the target item. Use `#fieldName#` placeholders. |
| `commands` | map | Source | Named command definitions for source connectors. |

---

## Sink connector

### Strategies

| Strategy class | Description |
|----------------|-------------|
| `Kafka.Connect.DynamoDb.Strategies.InsertStrategy` | Inserts each record as a new item using `PutItem`. |
| `Kafka.Connect.DynamoDb.Strategies.UpdateStrategy` | Updates an item matched by the partition/sort key. |
| `Kafka.Connect.DynamoDb.Strategies.DeleteStrategy` | Deletes an item matched by the partition/sort key. |
| `Kafka.Connect.DynamoDb.Strategies.UpsertStrategy` | Inserts or replaces an item using `PutItem`. |

### Example — upsert sink

```json
{
  "dynamodb-sink-connector": {
    "groupId": "dynamodb-sink-connector",
    "topics": ["events"],
    "tasks": 1,
    "plugin": {
      "type": "sink",
      "name": "dynamodb",
      "handler": "Kafka.Connect.DynamoDb.DynamoDbPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.DynamoDb.Strategies.UpsertStrategy"
      },
      "properties": {
        "region": "us-east-1",
        "accessKeyId": "YOUR_ACCESS_KEY",
        "secretAccessKey": "YOUR_SECRET_KEY",
        "tableName": "Events"
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
| `Kafka.Connect.DynamoDb.Strategies.ReadStrategy` | Scans or queries a DynamoDB table using cursor-based pagination. |
| `Kafka.Connect.DynamoDb.Strategies.StreamReadStrategy` | Reads change records from DynamoDB Streams. |

### Command configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | string | Yes | Kafka topic to publish records to. |
| `tableName` | string | Yes | DynamoDB table to read from. |
| `keys` | string[] | Yes | Partition key (and optionally sort key) attribute names. Used to build the Kafka message key. |
| `filters` | map | No | Additional filter conditions applied to the query. |
| `timestamp` | long | No | Unix epoch milliseconds of the last processed record (managed by the connector). |
| `timestampColumn` | string | No | Attribute name that holds the record timestamp for cursor-based incremental reads. |
| `indexName` | string | No | Name of a GSI to query instead of the base table. |
| `useStreams` | bool | No | `false` | When `true`, uses DynamoDB Streams instead of scan/query. Requires `streamArn`. |
| `streamArn` | string | No | ARN of the DynamoDB Stream to read from. Required when `useStreams` is `true`. |
| `shardIteratorType` | string | No | DynamoDB Streams shard iterator type: `TRIM_HORIZON`, `LATEST`, `AT_SEQUENCE_NUMBER`, or `AFTER_SEQUENCE_NUMBER`. |
| `sequenceNumber` | string | No | Sequence number for `AT_SEQUENCE_NUMBER` or `AFTER_SEQUENCE_NUMBER` iterator types. |

### Example — scan-based source

```json
{
  "dynamodb-source-connector": {
    "groupId": "dynamodb-source-connector",
    "tasks": 1,
    "plugin": {
      "type": "source",
      "name": "dynamodb",
      "handler": "Kafka.Connect.DynamoDb.DynamoDbPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.DynamoDb.Strategies.ReadStrategy"
      },
      "properties": {
        "region": "us-east-1",
        "accessKeyId": "YOUR_ACCESS_KEY",
        "secretAccessKey": "YOUR_SECRET_KEY",
        "commands": {
          "read-events": {
            "topic": "event-changes",
            "tableName": "Events",
            "keys": ["eventId"],
            "timestampColumn": "updatedAt",
            "timestamp": 0
          }
        }
      }
    }
  }
}
```

### Example — streams-based source

```json
{
  "dynamodb-streams-source-connector": {
    "groupId": "dynamodb-streams-source-connector",
    "tasks": 1,
    "plugin": {
      "type": "source",
      "name": "dynamodb",
      "handler": "Kafka.Connect.DynamoDb.DynamoDbPluginHandler",
      "strategy": {
        "name": "Kafka.Connect.DynamoDb.Strategies.StreamReadStrategy"
      },
      "properties": {
        "region": "us-east-1",
        "accessKeyId": "YOUR_ACCESS_KEY",
        "secretAccessKey": "YOUR_SECRET_KEY",
        "commands": {
          "stream-events": {
            "topic": "event-changes",
            "tableName": "Events",
            "keys": ["eventId"],
            "useStreams": true,
            "streamArn": "arn:aws:dynamodb:us-east-1:123456789012:table/Events/stream/2024-01-01T00:00:00.000",
            "shardIteratorType": "TRIM_HORIZON"
          }
        }
      }
    }
  }
}
```

> DynamoDB Streams must be enabled on the table with `NEW_AND_OLD_IMAGES` or `NEW_IMAGE` stream view type to capture full change records.
