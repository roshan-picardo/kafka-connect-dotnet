using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.MongoDb.Strategies;

namespace Kafka.Connect.MongoDb.Extensions
{
    public static class WriteModelStrategyProviderExtensions
    {
        public static IWriteModelStrategy GetWriteModelStrategy(this IEnumerable<IWriteModelStrategyProvider> providers,
            WriteStrategy config, MongoSinkRecord mongoSinkRecord)
        {
            config.Selector = string.IsNullOrEmpty(config.Selector) 
                ? config.Overrides != null && config.Overrides.Any() 
                    ? typeof(TopicWriteModelStrategyProvider).FullName 
                    : typeof(WriteModelStrategyProvider).FullName 
                : config.Selector;

            return providers.SingleOrDefault(p => p.GetType().FullName == config.Selector)
                ?.GetWriteModelStrategy(config, mongoSinkRecord);
        }
    }
}