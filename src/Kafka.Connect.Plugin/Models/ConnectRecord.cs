using System;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Kafka.Connect.Plugin.Models;

public interface IConnectRecord
{
    Exception Exception { get; set; }
    string Topic { get;  }
    int Partition { get;  }
    long Offset { get;  }
    Guid Key { get; }
    Status Status { get; set; }
}

public class ConnectRecord : IConnectRecord
{
    public LogTimestamp LogTimestamp { get; init; }
    protected ConnectRecord(){ }

    public ConnectRecord(string topic, int partition, long offset)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        LogTimestamp = new LogTimestamp();
        StartTiming();
    }
    
    public ConnectMessage<JsonNode> Deserialized { get; set; }
    
    public ConnectMessage<byte[]> Serialized { get; set; }

    public string Topic { get; protected set; }
    public int Partition { get; protected set; }
    public long Offset { get; protected set; }

    public Guid Key => new(MD5.HashData(Serialized.Key));
    public int Order { get; set; }

    public void Published(string topic, int partition, long offset)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        UpdateStatus();
    }

    public bool IsCommitReady(bool tolerated) => Status switch
    {
        Status.Inserted => true,
        Status.Deleted => true,
        Status.Updated => true,
        Status.Skipped => true,
        Status.Excluded => true,
        Status.Published => true,
        Status.Reviewed => true,
        Status.Failed => tolerated,
        _ => false
    };

    public Status Status { get; set; }

    public T GetKey<T>() => Deserialized.Key.Deserialize<T>();

    public T GetValue<T>() => Deserialized.Value.Deserialize<T>();

    public void UpdateStatus(bool failed = false)
    {
        Status = Status switch
        {
            Status.Processing => failed ? Status.Failed : Status.Processed,
            Status.Updating => failed ? Status.Failed : Status.Updated,
            Status.Skipping => failed ? Status.Failed : Status.Skipped,
            Status.Inserting => failed ? Status.Failed : Status.Inserted,
            Status.Deleting => failed ? Status.Failed : Status.Deleted,
            Status.Enriching => failed ? Status.Failed : Status.Enriched,
            Status.Publishing => failed ? Status.Failed : Status.Published,
            Status.Excluding => failed ? Status.Failed : Status.Excluded,
            Status.Sourcing => failed ? Status.Failed : Status.Sourced,
            Status.Selecting => failed ? Status.Failed : Status.Selected,
            _ => Status
        };
    }

    protected void StartTiming(long? millis = null)
    {
        LogTimestamp.Created = millis ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        LogTimestamp.Consumed = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public dynamic EndTiming(int batchSize, long? millis = null)
    {
        LogTimestamp.Committed = millis ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        LogTimestamp.BatchSize = batchSize;
        return new
        {
            Lag = LogTimestamp.Lag.ToString(@"dd\.hh\:mm\:ss\.fff"),
            Total = LogTimestamp.Total.ToString(@"dd\.hh\:mm\:ss\.fff"),
            LogTimestamp.Duration,
            Batch = new { Size = batchSize, Total = LogTimestamp.Batch }
        };
    }

    public Exception Exception { get; set; }

    public bool IsOf(string topic, int partition, long offset) =>
        topic == Topic && partition == Partition && offset == Offset;
    
    public bool Processing => Status is Status.Retrying or Status.Consumed or Status.Selected;
    public bool Sinking => Status is Status.Retrying or Status.Processed;
    public bool Publishing => Status is Status.Retrying or Status.Processed;
    public T Clone<T>() where T : ConnectRecord, new() => new T().Clone<T>(this);
    protected virtual T Clone<T>(ConnectRecord record) where T: ConnectRecord  => record as T;
}
