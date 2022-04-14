using Kafka.Connect.Mongodb.Models;

namespace Kafka.Connect.Mongodb
{
    public interface IWriteModelStrategyProvider
    {
        IWriteModelStrategy GetWriteModelStrategy(WriteStrategy config, MongoSinkRecord mongoSinkRecord);
    }
}