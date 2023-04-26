using Kafka.Connect.MongoDb.Models;

namespace Kafka.Connect.MongoDb
{
    public interface IWriteModelStrategyProvider
    {
        IWriteModelStrategy GetWriteModelStrategy(WriteStrategy config, MongoSinkRecord mongoSinkRecord);
    }
}