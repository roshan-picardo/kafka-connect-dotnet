using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Builders;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Utilities;
using Microsoft.Extensions.Logging;
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

        [OperationLog("Sending message to dead letter queue.")]
        public async Task Send(IEnumerable<SinkRecord> sinkRecords, Exception exception, string connector)
        {
            var topic = _configurationProvider.GetErrorsConfig(connector).Topic;
            using var producer = _kafkaClientBuilder.GetProducer(connector);
            {
                foreach (var record in sinkRecords)
                {
                    using (LogContext.Push(new PropertyEnricher("Topic", record.Topic),
                        new PropertyEnricher("Partition", record.Partition),
                        new PropertyEnricher("Offset", record.Offset)))
                    {
                        record.Consumed.Message.Headers.Add("_errorContext",
                            ByteConvert.Serialize(exception.ToString()));
                        record.Consumed.Message.Headers.Add("_sourceContext",
                            ByteConvert.Serialize(new MessageContext(record.TopicPartitionOffset)));

                        var delivered = await producer.ProduceAsync(topic, record.Consumed.Message);
                        _logger.LogInformation("{@Log}", new
                        {
                            Message = "Error message delivered.",
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