using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Connect.Config.Models;

namespace Kafka.Connect.Config
{
    public class PublisherConfig : ProducerConfig
    {
        public bool Enabled { get; init; }
        public string Connector { get; set; }
        public IEnumerable<ProcessorConfig> Enrichers { get; set; }
        
        public ConverterConfig Serializers { get; set; }
        public IEnumerable<TopicConfig> Topics { get; init; }

        public void MergePublisherConfigs(WorkerConfig workerConfig)
        {
            BootstrapServers ??= workerConfig.BootstrapServers;
            SecurityProtocol ??= workerConfig.SecurityProtocol;
            if (SecurityProtocol == Confluent.Kafka.SecurityProtocol.Ssl)
            {
                SslCaLocation ??= workerConfig.SslCaLocation;
                SslCertificateLocation ??= workerConfig.SslCertificateLocation;
                SslKeyLocation ??= workerConfig.SslKeyLocation;
                SslKeyPassword ??= workerConfig.SslKeyPassword;
                EnableSslCertificateVerification ??= workerConfig.EnableSslCertificateVerification;
            }
        }
    }
}