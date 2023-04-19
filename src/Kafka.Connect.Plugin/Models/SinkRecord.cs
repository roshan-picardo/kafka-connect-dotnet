using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Models
{
    public class SinkRecord
    {
        private readonly LogTimestamp _logTimestamp;

        protected SinkRecord(string topic, int partition, long offset)
        {
            _logAttributes = new Dictionary<string, object>();
            _calcAttributes = new Dictionary<string, Func<object>>();

            Topic = topic;
            Partition = partition;
            Offset = offset;
            Status = SinkStatus.Consumed;
            _logTimestamp = new LogTimestamp();
        }

        public void Parsed(JToken key, JToken value)
        {
            Message = new JObject
            {
                {Constants.Key, key?[Constants.Key]},
                {Constants.Value, value?[Constants.Value]}
            };
        }

        public JToken Message { get; set; }
       
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

        public T GetKey<T>() => JsonConvert.DeserializeObject<T>(Key?.ToString() ?? string.Empty);

        public T GetValue<T>() => JsonConvert.DeserializeObject<T>(Value?.ToString() ?? string.Empty);

        public T GetMessage<T>() => JsonConvert.DeserializeObject<T>(Message?.ToString() ?? string.Empty);

        public JToken Key =>  Message?[Constants.Key];
        public JToken Value => Message?[Constants.Value];

        public void UpdateStatus()
        {
            Status = Status switch
            {
                SinkStatus.Updating => SinkStatus.Updated,
                SinkStatus.Skipping => SinkStatus.Skipped,
                SinkStatus.Inserting => SinkStatus.Inserted,
                SinkStatus.Deleting => SinkStatus.Deleted,
                SinkStatus.Enriching => SinkStatus.Enriched,
                SinkStatus.Publishing => SinkStatus.Published,
                SinkStatus.Excluding => SinkStatus.Excluded,
                _ => Status
            };
        }

        private readonly IDictionary<string, dynamic> _logAttributes;
        private readonly IDictionary<string, Func<dynamic>> _calcAttributes;
        
        public void AddLog(string key, object d)
        {
            if (_logAttributes.ContainsKey(key))
            {
                _logAttributes[key] = d;
            }
            else
            {
                _logAttributes.Add(key, d);
            }
        }

        public void AddLog(string key, Func<object> data)
        {
            if (_calcAttributes.ContainsKey(key))
            {
                _calcAttributes[key] = data;
            }
            else
            {
                _calcAttributes.Add(key, data);
            }
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