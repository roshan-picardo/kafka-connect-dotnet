using System.Collections.Generic;
using Confluent.Kafka;

namespace Kafka.Connect.Configurations
{
    public interface IConfigurationProvider
    {
        FailOverConfig GetFailOverConfig();
        HealthCheckConfig GetHealthCheckConfig();
        ConsumerConfig GetConsumerConfig(string connector = null);
        ProducerConfig GetProducerConfig(string connector = null);
        ConnectorConfig GetConnectorConfig(string connector);
        IList<ConnectorConfig> GetConnectorConfigs(bool includeDisabled = false);
        string GetWorkerName();
        RestartsConfig GetRestartsConfig();
        ErrorsConfig GetErrorsConfig(string connector);
        RetryConfig GetRetriesConfig(string connector);
        EofConfig GetEofSignalConfig(string connector);
        BatchConfig GetBatchConfig(string connector);
        IList<string> GetTopics(string connector);
        (string keyConverter, string valueConverter) GetMessageConverters(string connector, string topic);
        IList<ProcessorConfig> GetMessageProcessors(string connector, string topic);
        SinkConfig GetSinkConfig(string connector);
        bool IsErrorTolerated(string connector);
        bool IsDeadLetterEnabled(string connector);
        (bool enableAutoCommit, bool enableAutoOffsetStore) GetAutoCommitConfig();
        string GetGroupId(string connector);
        void Validate();
    }
}