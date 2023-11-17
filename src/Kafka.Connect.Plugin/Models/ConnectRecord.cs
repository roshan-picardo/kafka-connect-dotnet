using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Models
{
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

        public ConnectMessage<JToken> DeserializedToken { get; set; }
        
        public ConnectMessage<JsonNode> Deserialized { get; set; }
        
        public ConnectMessage<byte[]> Serialized { get; set; }
        
        public ConnectMessage<IDictionary<string, object>> Flattened { get; set; }

        public string Topic { get; }
        public int Partition { get; }
        public long Offset { get; }

        // indicate the record to stop processing
        public bool Skip { get; set; } 
        
        public bool CanCommitOffset { get; set; }
        
        private SinkStatus _status;

        public SinkStatus Status
        {
            get => _status;
            set
            {
                switch (value)
                {
                    case SinkStatus.Processed:
                        IsProcessed = true;
                        break;
                    case SinkStatus.Enriched:
                    case SinkStatus.Excluded:
                        IsEnriched = true;
                        break;
                    case SinkStatus.Published:
                        IsPublished = true;
                        break;
                    case SinkStatus.Updated:
                    case SinkStatus.Deleted:
                    case SinkStatus.Inserted:
                    case SinkStatus.Skipped:
                        IsSaved = true;
                        break;
                }
                _status = value;
            }
        }

        public T GetKey<T>() => JsonConvert.DeserializeObject<T>(DeserializedToken.Key?.ToString() ?? string.Empty);

        public T GetValue<T>() => JsonConvert.DeserializeObject<T>(DeserializedToken.Value?.ToString() ?? string.Empty);

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

        public bool IsProcessed { get; private set; }
        public bool IsSaved { get; private set; }
        public bool IsEnriched { get; private set; }
        public bool IsPublished { get; private set; }
        
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
}