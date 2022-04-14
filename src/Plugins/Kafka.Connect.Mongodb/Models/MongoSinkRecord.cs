using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Mongodb.Models
{
    public class MongoSinkRecord
    {
        private MongoSinkRecord(SinkRecord sinkRecord)
        {
            SinkRecord = sinkRecord;
        }

        public static MongoSinkRecord Create(SinkRecord record)
        {
            return new MongoSinkRecord(record);
        }

        public SinkRecord SinkRecord
        {
            get;
        }
        public IEnumerable<WriteModel<BsonDocument>> WriteModels { get; set; }

        public bool ReadyToWrite
        {
            get
            {
                if (SinkRecord.Skip) return false;
                return WriteModels != null && WriteModels.Any();
            }
        }

        // Lets decorate the required properties 
        public bool Skip => SinkRecord.Skip;
        public SinkStatus Status
        {
            set => SinkRecord.Status = value;
            get => SinkRecord.Status;
        }
        public string Topic => SinkRecord.Topic;
        public int Partition => SinkRecord.Partition;
        public long Offset => SinkRecord.Offset;
        public JToken Data => SinkRecord.Data;
        public bool CanCommitOffset
        {
            set => SinkRecord.CanCommitOffset = value;
        }
        
        public void UpdateStatus()
        {
            SinkRecord.UpdateStatus();
        }
        
        public T GetKey<T>()
        {
            return SinkRecord.GetKey<T>();
        }

        public T GetValue<T>()
        {
            return SinkRecord.GetValue<T>();
        }

        public void AddLog(string key, object data)
        {
            SinkRecord.AddLog(key, data);
        }
        public void AddLog(string key, Func<object> data)
        {
            SinkRecord.AddLog(key, data);
        }
    }
}