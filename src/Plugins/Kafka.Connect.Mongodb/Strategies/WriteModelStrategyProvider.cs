using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Mongodb.Models;
using Kafka.Connect.Plugin.Logging;

namespace Kafka.Connect.Mongodb.Strategies
{
    public class WriteModelStrategyProvider : IWriteModelStrategyProvider
    {
        private readonly ILogger<WriteModelStrategyProvider> _logger;
        private readonly IEnumerable<IWriteModelStrategy> _writeModelStrategies;

        public WriteModelStrategyProvider(ILogger<WriteModelStrategyProvider> logger, IEnumerable<IWriteModelStrategy> writeModelStrategies)
        {
            _logger = logger;
            _writeModelStrategies = writeModelStrategies;
        }

        public IWriteModelStrategy GetWriteModelStrategy(WriteStrategy config, MongoSinkRecord mongoSinkRecord)
        {
            using (_logger.Track("Get write model strategy"))
            {
                return _writeModelStrategies.SingleOrDefault(s => s.GetType().FullName == config.Name);
            }
        }
    }
}