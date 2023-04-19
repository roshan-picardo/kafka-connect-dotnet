using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Mongodb.Models
{
    public class MongoSinkRecord : SinkRecord<IEnumerable<WriteModel<BsonDocument>>>
    {
        private readonly SinkRecord _sinkRecord;

        public MongoSinkRecord(SinkRecord sinkRecord) : base(sinkRecord)
        {
            _sinkRecord = sinkRecord;
        }

        public override bool Ready
        {
            get
            {
                if (_sinkRecord.Skip) return false;
                return Models != null && Models.Any();
            }
        }

        public override object LogModels()
        {
            return Models.Select(m => new
            {
                Type = m.ModelType,
                Model = JObject.Parse(JsonConvert.SerializeObject(m))
            });
        }
    }
}