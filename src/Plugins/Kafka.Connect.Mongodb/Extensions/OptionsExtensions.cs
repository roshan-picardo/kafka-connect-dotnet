using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Mongodb.Models;

namespace Kafka.Connect.Mongodb.Extensions
{
    public static class OptionsExtensions
    {
        public static IList<SinkConfig<MongoSinkConfig>> Merged(this IList<SinkConfig<MongoSinkConfig>> options, SinkConfig<MongoSinkConfig> option)
        {
            if (option == null)
            {
                return options;
            }

            if (options == null)
            {
                return new[] {option};
            }

            var merged = new List<SinkConfig<MongoSinkConfig>>();
            foreach (var config in options.ToArray())
            {
                var sinkConfig = config ?? option;

                sinkConfig.Sink ??= option.Sink ?? new MongoSinkConfig();
                sinkConfig.Sink.Properties ??=  new Properties();
                if (option?.Sink?.Properties != null)
                {
                    var props = option.Sink.Properties;
                    sinkConfig.Sink.Properties.Collection ??= props.Collection;
                    sinkConfig.Sink.Properties.Database ??= props.Database;
                    sinkConfig.Sink.Properties.Username ??= props.Username;
                    sinkConfig.Sink.Properties.Password ??= props.Password;
                    sinkConfig.Sink.Properties.ConnectionUri ??= props.ConnectionUri;
                    sinkConfig.Sink.Properties.WriteStrategy ??= props.WriteStrategy;
                }
                merged.Add(sinkConfig);
            }
            return merged;
        }
    }
}