using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Models
{
    public class SinkRecord
    {
        public SinkRecord(ConsumeResult<byte[], byte[]> consumed, JToken key = null, JToken value = null)
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
            Topic = consumed.Topic;
            Partition = consumed.Partition.Value;
            Offset = consumed.TopicPartitionOffset.Offset.Value;
            Headers = consumed.Message.Headers;
            TopicPartitionOffset = consumed.TopicPartitionOffset;
            Status = SinkStatus.Consumed;
        }

        protected SinkRecord()
        {
            
        }
        protected SinkRecord This => this;

        public static SinkRecord New(ConsumeResult<byte[], byte[]> consumed, JToken key = null, JToken value = null)
        {
            return new SinkRecord(consumed, key, value);
        }
        
        public void Parsed(JToken key, JToken value)
        {
            Data = new JObject
            {
                {Constants.Key, key?[Constants.Key]},
                {Constants.Value, value?[Constants.Value]}
            };
        }

        public TopicPartitionOffset TopicPartitionOffset { get; }
        
        public JToken Data { get; set; }
       
        public string Topic { get; }
        public int Partition { get; }
        public long Offset { get; }
        public Headers Headers { get; }
        
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
        
        public IDictionary<string, Message<byte[], byte[]>> PublishMessages { get; set; }

        public bool CanPublish
        {
            get
            {
                if (PublishMessages == null || !PublishMessages.Any()) return false;
                return Status switch
                {
                    SinkStatus.Enriched => true,
                    SinkStatus.Failed => true,
                    SinkStatus.Published => false,
                    SinkStatus.Skipped => false,
                    SinkStatus.Excluded => false,
                    _ => false
                };
            }
        }

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

        public IDictionary<string, object> GetLogs()
        {
            _logAttributes.Add("Status", Status);
            foreach (var attribute in _calcAttributes)
            {
                if (attribute.Value != null)
                {
                    AddLog(attribute.Key, attribute.Value());
                }
            }

            return _logAttributes;
        }
        
        public bool IsProcessed { get; private set; }
        public bool IsSaved { get; private set; }
        public bool IsEnriched { get; private set; }
        public bool IsPublished { get; private set; }
    }
}