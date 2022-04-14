using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Config.Models;
using Serilog;

namespace Kafka.Connect.Config
{
    public class ConnectorConfig : ConsumerConfig
    {

        public string Name { get; init; }
        public int? MaxTasks { get; init; }

        public string Topic { get; set; }

        public IList<string> Topics { get; set; }

        public SelfHealingConfig SelfHealing { get; set; }
        public IEnumerable<ProcessorConfig> Processors { get; set; }

        public SinkConfig Sink { get; init; }

        public ErrorConfig Errors { get; set; }

        public EndOfPartitionConfig EndOfPartition { get; init; }

        public string Plugin { get; init; }

        public PublisherConfig Publisher { get; set; }

        public void MergeSelfHealingSettings(SelfHealingConfig workerConfig)
        {
            SelfHealing ??= workerConfig ?? new SelfHealingConfig {Attempts = -1};
            SelfHealing.Attempts = SelfHealing.Attempts == 0 ? -1 : SelfHealing.Attempts;
        }

        public void MergeSecurityProtocolSettings(WorkerConfig workerConfig)
        {
            SecurityProtocol ??= workerConfig.SecurityProtocol;
            if (SecurityProtocol != Confluent.Kafka.SecurityProtocol.Ssl) return;
            SslCaLocation ??= workerConfig.SslCaLocation;
            SslCertificateLocation ??= workerConfig.SslCertificateLocation;
            SslKeyLocation ??= workerConfig.SslKeyLocation;
            SslKeyPassword ??= workerConfig.SslKeyPassword;
            EnableSslCertificateVerification ??= workerConfig.EnableSslCertificateVerification;
        }

        public void MergeErrorToleranceSettings(ErrorConfig workerConfig)
        {
            Errors ??= workerConfig ?? new ErrorConfig();
            // ignore dead letter if Tolerance is None
            Errors.DeadLetter = Errors.Tolerance == ErrorTolerance.None
                ? new DeadLetterConfig()
                : Errors.DeadLetter;
        }

        public void MergeTopicSettings()
        {
            if (string.IsNullOrWhiteSpace(Topic)) return;
            Log.ForContext<ConnectorConfig>().Warning("{@config}",
                "'Topic' configuration is marked for deprecation, consider using 'Topics' instead.");
            Topics ??= new List<string>();
            Topics.Add(Topic);
        }

        public void MergeBootstrapSettings(WorkerConfig workerConfig)
        {
            BootstrapServers ??= workerConfig.BootstrapServers;
            GroupId ??= Name;
            ClientId ??= Name;
            AutoOffsetReset ??= workerConfig.AutoOffsetReset;
            PartitionAssignmentStrategy ??= workerConfig.PartitionAssignmentStrategy;
            FetchWaitMaxMs ??= workerConfig.FetchWaitMaxMs;
        }

        public void MergeCommitSettings(WorkerConfig workerConfig)
        {
            EnableAutoCommit ??= workerConfig.EnableAutoCommit;
            EnableAutoOffsetStore ??= workerConfig.EnableAutoOffsetStore;
            EnablePartitionEof ??= workerConfig.EnablePartitionEof;
            SessionTimeoutMs ??= workerConfig.SessionTimeoutMs;
            AutoCommitIntervalMs ??= workerConfig.AutoCommitIntervalMs;
        }

        public void MergePublishers(IEnumerable<PublisherConfig> publishers)
        {
            if (publishers == null)
            {
                return;
            }

            Publisher = publishers.SingleOrDefault(p => p.Connector == Name && p.Enabled);
        }

        public void MergeProcessorTopicOverrides()
        {
            if(Processors == null || !Processors.Any()) return;
            foreach (var processor in Processors)
            {
                if (!string.IsNullOrWhiteSpace(processor.Topic))
                {
                    processor.Topics = new[] {processor.Topic};
                }
            }
        }
    }
}