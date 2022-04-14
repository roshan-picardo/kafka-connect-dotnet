using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Mongodb.Models;

namespace Kafka.Connect.Mongodb.Strategies
{
    public class WriteModelStrategyProvider : IWriteModelStrategyProvider
    {
        private readonly IEnumerable<IWriteModelStrategy> _writeModelStrategies;

        public WriteModelStrategyProvider(IEnumerable<IWriteModelStrategy> writeModelStrategies)
        {
            _writeModelStrategies = writeModelStrategies;
        }

        public IWriteModelStrategy GetWriteModelStrategy(WriteStrategy config, MongoSinkRecord mongoSinkRecord)
        {
            return _writeModelStrategies.SingleOrDefault(s => s.GetType().FullName == config.Name);
        }
    }
}