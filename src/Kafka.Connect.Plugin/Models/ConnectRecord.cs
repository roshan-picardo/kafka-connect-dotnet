using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Kafka.Connect.Plugin.Models;

public class ConnectRecord
{
    private readonly LogTimestamp _logTimestamp;

    protected ConnectRecord(string topic, int partition, long offset)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Status = SinkStatus.Consumed;
        _logTimestamp = new LogTimestamp();
    }

    public ConnectMessage<JsonNode> Deserialized { get; set; }
        
    public ConnectMessage<byte[]> Serialized { get; set; }
        
    public ConnectMessage<IDictionary<string, object>> Flattened { get; set; }

    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }

    // indicate the record to stop processing
    public bool Skip { get; set; } 
        
    public bool CanCommitOffset { get; set; }

    public SinkStatus Status { get; set; }

    public T GetKey<T>() => Deserialized.Key.Deserialize<T>();

    public T GetValue<T>() => Deserialized.Value.Deserialize<T>();

    public void UpdateStatus(bool failed = false)
    {
        Status = Status switch
        {
            SinkStatus.Updating => failed ? SinkStatus.Failed : SinkStatus.Updated,
            SinkStatus.Skipping => failed ? SinkStatus.Failed : SinkStatus.Skipped,
            SinkStatus.Inserting => failed ? SinkStatus.Failed : SinkStatus.Inserted,
            SinkStatus.Deleting => failed ? SinkStatus.Failed : SinkStatus.Deleted,
            SinkStatus.Enriching => failed ? SinkStatus.Failed : SinkStatus.Enriched,
            SinkStatus.Publishing => failed ? SinkStatus.Failed : SinkStatus.Published,
            SinkStatus.Excluding => failed ? SinkStatus.Failed : SinkStatus.Excluded,
            _ => Status
        };
    }

    public bool IsOperationCompleted { get; set; }

    protected void StartTiming(long? millis = null)
    {
        _logTimestamp.Created = millis ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _logTimestamp.Consumed = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public dynamic EndTiming(int batchSize, long? millis = null)
    {
        _logTimestamp.Committed = millis ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _logTimestamp.BatchSize = batchSize;
        return new
        {
            Lag = _logTimestamp.Lag.ToString(@"dd\.hh\:mm\:ss\.fff"),
            Total = _logTimestamp.Total.ToString(@"dd\.hh\:mm\:ss\.fff"),
            _logTimestamp.Duration,
            Batch = new { Size = batchSize, Total = _logTimestamp.Batch }
        };
    }
}