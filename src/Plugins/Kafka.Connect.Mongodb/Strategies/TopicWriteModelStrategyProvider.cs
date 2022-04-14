using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Mongodb.Models;

namespace Kafka.Connect.Mongodb.Strategies
{
    public class TopicWriteModelStrategyProvider : IWriteModelStrategyProvider
    {
        private readonly IEnumerable<IWriteModelStrategy> _writeModelStrategies;

        public TopicWriteModelStrategyProvider(IEnumerable<IWriteModelStrategy> writeModelStrategies)
        {
            _writeModelStrategies = writeModelStrategies;
        }

        public IWriteModelStrategy GetWriteModelStrategy(WriteStrategy config, MongoSinkRecord mongoSinkRecord)
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