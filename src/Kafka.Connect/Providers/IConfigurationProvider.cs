using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Connect.Configurations;

namespace Kafka.Connect.Providers;

public interface IConfigurationProvider
{
    FailOverConfig GetFailOverConfig();
    HealthCheckConfig GetHealthCheckConfig();
    ConsumerConfig GetConsumerConfig(string connector = null);
    ProducerConfig GetProducerConfig(string connector = null);
    ConnectorConfig GetConnectorConfig(string connector);
    IList<ConnectorConfig> GetAllConnectorConfigs(bool includeDisabled = false);
    string GetWorkerName();
    RestartsConfig GetRestartsConfig();
    ErrorsConfig GetErrorsConfig(string connector);
    RetryConfig GetRetriesConfig(string connector);
    EofConfig GetEofSignalConfig(string connector);
    BatchConfig GetBatchConfig(string connector);
    IList<string> GetTopics(string connector);
    ConverterConfig GetDeserializers(string connector, string topic);
    ConverterConfig GetSerializers(string connector, string topic);
    IList<ProcessorConfig> GetMessageProcessors(string connector, string topic);
    SinkConfig GetSinkConfig(string connector);
    SourceConfig GetSourceConfig(string connector);
    bool IsErrorTolerated(string connector);
    bool IsDeadLetterEnabled(string connector);
    (bool EnableAutoCommit, bool EnableAutoOffsetStore) GetAutoCommitConfig();
    string GetGroupId(string connector);
    string GetLogEnhancer(string connector);
    void Validate();
}
