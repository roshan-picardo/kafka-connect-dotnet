using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Logging;

namespace Kafka.Connect.MongoDb.Strategies
{
    public class TopicWriteModelStrategyProvider : IWriteModelStrategyProvider
    {
        private readonly ILogger<TopicWriteModelStrategyProvider> _logger;
        private readonly IEnumerable<IWriteModelStrategy> _writeModelStrategies;

        public TopicWriteModelStrategyProvider(ILogger<TopicWriteModelStrategyProvider> logger, IEnumerable<IWriteModelStrategy> writeModelStrategies)
        {
            _logger = logger;
            _writeModelStrategies = writeModelStrategies;
        }

        public IWriteModelStrategy GetWriteModelStrategy(WriteStrategy config, MongoSinkRecord mongoSinkRecord)
        {
            using (_logger.Track("Get write model strategy"))
            {
                if (config.Overrides == null || !config.Overrides.Any())
                {
                    return _writeModelStrategies.SingleOrDefault(w => w is TopicSkipWriteModelStrategy);
                }

                var topicOverride = config.Overrides.SingleOrDefault(t => t.Key == mongoSinkRecord?.Topic).Value;
                if (!string.IsNullOrWhiteSpace(topicOverride))
                    return _writeModelStrategies.SingleOrDefault(s => s.GetType().FullName == topicOverride);

                return string.IsNullOrWhiteSpace(config.Name)
                    ? _writeModelStrategies.SingleOrDefault(w => w is TopicSkipWriteModelStrategy)
                    : _writeModelStrategies.SingleOrDefault(s => s.GetType().FullName == config.Name);
            }
        }
    }
}