using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Extensions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Models
{
    public class SinkRecord
    {
        public SinkRecord(ConsumeResult<byte[], byte[]> consumed, string topic, int partition, long offset, JToken key = null, JToken value = null)
        {
            if (key != null || value != null)
            {
                Data = new JObject
                {
                    {Constants.Key, key?[Constants.Key]},
                    {Constants.Value, value?[Constants.Value]}
                };
            }
            _logAttributes = new Dictionary<string, object>();
            _calcAttributes = new Dictionary<string, Func<object>>();
            
            if (consumed == null) return;
            Consumed = consumed;
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Headers = consumed.Message.Headers;
            Status = SinkStatus.Consumed;
        }

        protected SinkRecord()
        {
            
        }
        protected SinkRecord This => this;

        public static SinkRecord New(ConsumeResult<byte[], byte[]> consumed, JToken key = null, JToken value = null)
        {
            return new SinkRecord(consumed, consumed.Topic, consumed.Partition, consumed.Offset, key, value);
        }
        
        public void Parsed(JToken key, JToken value)
        {
            Data = new JObject
            {
                {Constants.Key, key?[Constants.Key]},
                {Constants.Value, value?[Constants.Value]}
            };
        }

        public JToken Data { get; set; }
       
        public string Topic { get; }
        public int Partition { get; }
        public long Offset { get; }
        private Headers Headers
        {
            get => Consumed.Message.Headers;
            set => Consumed.Message.Headers = value;
        }
        
        // indicate the record to stop processing
        public bool Skip { get; set; } 
        
        public bool CanCommitOffset { get; set; }
        
        public ConsumeResult<byte[], byte[]> Consumed { get; }

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

        public T GetKey<T>()
        {
            return JsonConvert.DeserializeObject<T>(Key?.ToString() ?? string.Empty);
        }

        public T GetValue<T>()
        {
            return JsonConvert.DeserializeObject<T>(Value?.ToString() ?? string.Empty);
        }

        public JToken Key =>  Data[Constants.Key];
        public JToken Value => Data[Constants.Value];

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
        
        private dynamic SetTime(Action<LogTimestamp> setTime)
        {
            Headers ??= new Headers();
            LogTimestamp logTimestamp;
            var timestamp = Headers.SingleOrDefault(h => h.Key == "_logTimestamp");
            if (timestamp != null)
            {
                Headers.Remove("_logTimestamp");
                logTimestamp = ByteConvert.Deserialize<LogTimestamp>(timestamp.GetValueBytes());
            }
            else
            {
                logTimestamp = new LogTimestamp();
            }

            setTime(logTimestamp);
            Headers.Add("_logTimestamp", ByteConvert.Serialize(logTimestamp));
            return logTimestamp;
        }
        
        public void StartTiming(long? millis = null)
        {
            SetTime(t =>
            {
                t.Created = millis ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                t.Consumed = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            });
        }

        public dynamic EndTiming(int batchSize)
        {
            var logTimestamp = SetTime(t =>
            {
                t.Committed = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                t.BatchSize = batchSize;
            });
            return new
            {
                Lag = logTimestamp.Lag.ToString(@"dd\.hh\:mm\:ss\.fff"),
                Total = logTimestamp.Total.ToString(@"dd\.hh\:mm\:ss\.fff"),
                logTimestamp.Duration,
                Batch = new { Size = batchSize, Total = logTimestamp.Batch }
            };
        }
    }
}