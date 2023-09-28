using Kafka.Connect.Plugin.Models;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Models
{
    public class MongoSinkRecord : SinkRecord<WriteModel<BsonDocument>>
    {
        private readonly SinkRecord _sinkRecord;

        public MongoSinkRecord(SinkRecord sinkRecord) : base(sinkRecord)
        {
            _sinkRecord = sinkRecord;
        }
    }
}