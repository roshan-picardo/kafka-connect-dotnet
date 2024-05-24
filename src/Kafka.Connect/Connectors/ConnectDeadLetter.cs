using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Builders;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Connectors
{
    public class ConnectDeadLetter : IConnectDeadLetter
    {
        private readonly ILogger<ConnectDeadLetter> _logger;
        private readonly IKafkaClientBuilder _kafkaClientBuilder;
        private readonly IConfigurationProvider _configurationProvider;

        public ConnectDeadLetter(ILogger<ConnectDeadLetter> logger, IKafkaClientBuilder kafkaClientBuilder, IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _kafkaClientBuilder = kafkaClientBuilder;
            _configurationProvider = configurationProvider;
        }

        public async Task Send(IEnumerable<SinkRecord> sinkRecords, Exception exception, string connector)
        {
            using (_logger.Track("Sending message to dead letter queue."))
            {
                var topic = _configurationProvider.GetErrorsConfig(connector).Topic;
                using var producer = _kafkaClientBuilder.GetProducer(connector);
                {
                    foreach (var record in sinkRecords)
                    {
                        using (ConnectLog.TopicPartitionOffset(record.Topic, record.Partition, record.Offset))
                        {
                            var delivered = await producer.ProduceAsync(topic, record.GetDeadLetterMessage(exception));
                            _logger.Info("Error message delivered.", new
                            {
                                delivered.Topic,
                                Partition = delivered.Partition.Value,
                                Offset = delivered.Offset.Value
                            });
                        }
                    }
                }
            }
        }
    }
}