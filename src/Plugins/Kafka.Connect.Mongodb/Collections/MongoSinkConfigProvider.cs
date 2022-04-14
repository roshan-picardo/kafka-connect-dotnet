using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Mongodb.Extensions;
using Kafka.Connect.Mongodb.Models;
using Microsoft.Extensions.Configuration;

namespace Kafka.Connect.Mongodb.Collections
{
    public class MongoSinkConfigProvider : IMongoSinkConfigProvider
    {
        private readonly IList<SinkConfig<MongoSinkConfig>> _connectorOptions;

        public MongoSinkConfigProvider(IConfiguration configuration)
        {
            _connectorOptions = configuration.GetSection("worker:connectors").Get<List<SinkConfig<MongoSinkConfig>>>();
            if (_connectorOptions.Any(o => o.Sink?.Properties?.Collection == null))
            {
                _connectorOptions = MergeIfMissing(configuration.GetSection("worker:connectors")
                    .Get<List<SinkConfig<MongoSinkConfig>>>(o => o.BindNonPublicProperties = true));
            }

            var workerOptions = configuration.GetSection("worker").Get<SinkConfig<MongoSinkConfig>>();

            if (workerOptions?.Sink?.Properties != null && workerOptions.Sink.Properties.Collection == null)
            {
                workerOptions = configuration.GetSection("worker")
                    .Get<SinkConfig<MongoSinkConfig>>(o => o.BindNonPublicProperties = true);
            }

            _connectorOptions = _connectorOptions.Merged(workerOptions);
        }

        public MongoSinkConfig GetMongoSinkConfig(string connector)
        {
            var sinkConfig = _connectorOptions?.SingleOrDefault(c => c.Name == connector)?.Sink;
            (sinkConfig ??= new MongoSinkConfig()).Connector = connector;
            return sinkConfig;
        }

        private IList<SinkConfig<MongoSinkConfig>> MergeIfMissing(IList<SinkConfig<MongoSinkConfig>> options)
        {
            foreach (var option in _connectorOptions)
            {
                var stringOption = options.SingleOrDefault(o => o.Name == option.Name);
                if(stringOption?.Sink?.Properties == null || option?.Sink?.Properties == null) continue;
                option.Sink.Properties.Collection ??= stringOption.Sink.Properties.Collection;
            }
            return _connectorOptions;
        }
    }
}