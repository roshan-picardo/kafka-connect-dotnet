using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.Mongodb.Strategies
{
    public class TopicSkipWriteModelStrategy : IWriteModelStrategy
    {
        private readonly ILogger<TopicSkipWriteModelStrategy> _logger;

        public TopicSkipWriteModelStrategy(ILogger<TopicSkipWriteModelStrategy> logger)
        {
            _logger = logger;
        }
        public async Task<(SinkStatus, IEnumerable<WriteModel<BsonDocument>>)> CreateWriteModels(SinkRecord sinkRecord)
        {
            using (_logger.Track("Creating write models"))
            {
                sinkRecord.Skip = true;
                return await Task.FromResult<(SinkStatus, IEnumerable<WriteModel<BsonDocument>>)>((SinkStatus.Skipping,
                    Enumerable.Empty<WriteModel<BsonDocument>>()));
            }
        }
    }
}