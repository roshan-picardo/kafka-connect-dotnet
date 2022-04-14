using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Connect.Builders;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Configurations;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Utilities;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using Serilog.Core.Enrichers;
using ConnectorConfig = Kafka.Connect.Config.ConnectorConfig;
using ErrorTolerance = Kafka.Connect.Config.Models.ErrorTolerance;

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

        public async Task CreateTopic(ConnectorConfig connectorConfig)
        {
            connectorConfig.Errors ??= new ErrorConfig();
            var deadLetterConfig = connectorConfig.Errors.DeadLetter ??= new DeadLetterConfig();
            if (connectorConfig.Errors.Tolerance == ErrorTolerance.All && (deadLetterConfig.Create ?? false) &&
                !string.IsNullOrWhiteSpace(deadLetterConfig.Topic))
            {
                using var adminClient = _kafkaClientBuilder.GetAdminClient(connectorConfig);
                {
                    try
                    {
                        await adminClient.CreateTopicsAsync(new List<TopicSpecification>
                        {
                            new TopicSpecification
                            {
                                Name = deadLetterConfig.Topic,
                                ReplicationFactor = deadLetterConfig.Replication ?? 3,
                                NumPartitions = deadLetterConfig.Partitions ?? 1
                            }
                        });
                    }
                    catch (CreateTopicsException cte)
                    {
                        var topicReport = cte.Results?.SingleOrDefault(r =>
                            r.Topic == deadLetterConfig.Topic && r.Error.Code == ErrorCode.TopicAlreadyExists);

                        if (topicReport != null && topicReport.Error == ErrorCode.TopicAlreadyExists)
                        {
                            _logger.LogDebug("{@Log}", new {Message = "Topic already exists."});
                            return;
                        }

                        _logger.LogError(cte, "{@Log}", new {Message = "Failed to create the dead letter topic, possibly the topic already exists."});
                    }
                }
            }
        }

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

                        var delivered = await _logger.Timed("Delivering errored message.")
                            .Execute(async () =>
                                await producer.ProduceAsync(topic, record.Consumed.Message));
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