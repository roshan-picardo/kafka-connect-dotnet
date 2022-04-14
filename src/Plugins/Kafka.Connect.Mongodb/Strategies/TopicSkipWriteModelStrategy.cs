using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.Mongodb.Strategies
{
    public class TopicSkipWriteModelStrategy : IWriteModelStrategy
    {
        public async Task<(SinkStatus, IEnumerable<WriteModel<BsonDocument>>)> CreateWriteModels(SinkRecord sinkRecord)
        {
            sinkRecord.Skip = true;
            return await Task.FromResult<(SinkStatus, IEnumerable<WriteModel<BsonDocument>>)>((SinkStatus.Skipping,
                Enumerable.Empty<WriteModel<BsonDocument>>()));
        }
    }
}