using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Models;
using ConnectorConfig = Kafka.Connect.Configurations.ConnectorConfig;

namespace Kafka.Connect.Providers;

public interface IConfigurationProvider
{
    LeaderConfig GetLeaderConfig(bool reload = false);
    void ReloadLeaderConfig();
    FailOverConfig GetFailOverConfig();
    HealthCheckConfig GetHealthCheckConfig();
    ConsumerConfig GetConsumerConfig(string connector = null);
    ProducerConfig GetProducerConfig(string connector = null);
    ConnectorConfig GetConnectorConfig(string connector);
    IList<ConnectorConfig> GetAllConnectorConfigs(bool includeDisabled = false);
    string GetNodeName();
    RestartsConfig GetRestartsConfig();
    ErrorsConfig GetErrorsConfig(string connector);
    RetryConfig GetRetriesConfig(string connector);
    EofConfig GetEofSignalConfig(string connector);
    BatchConfig GetBatchConfig(string connector);
    int GetDegreeOfParallelism(string connector);
    ParallelRetryOptions GetParallelRetryOptions(string connector);
    IList<string> GetTopics(string connector);
    InternalTopicConfig GetTopics();
    ConverterConfig GetMessageConverters(string connector, string topic);
    IList<ProcessorConfig> GetMessageProcessors(string connector, string topic);
    PluginConfig GetPluginConfig(string connector);
    bool IsErrorTolerated(string connector);
    bool IsDeadLetterEnabled(string connector);
    (bool EnableAutoCommit, bool EnableAutoOffsetStore) GetAutoCommitConfig();
    string GetGroupId(string connector);
    string GetLogEnhancer(string connector);
    void Validate();
    bool IsLeader { get; }
    bool IsWorker { get; }
    InitializerConfig GetPlugin(string connector);
}
