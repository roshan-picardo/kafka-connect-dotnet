using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Config.Models;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Config
{
    public class WorkerConfig : ConsumerConfig
    {
        // Set of connectors
        public string Name { get; set; }
        public IEnumerable<ConnectorConfig> Connectors { get; set; }
        
        public IEnumerable<PublisherConfig> Publishers { get; set; }
        
        public IEnumerable<PluginConfig> Plugins { get; set; }
        
        public ErrorConfig Errors { get; set; }
        
        public SelfHealingConfig SelfHealing { get; set; }
        public FailOverConfig FailOver { get; set; }

        
        public void MergeAndVerify(ILogger logger)
        {
            try
            {
                Merge();
                Verify();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Configuration check failed.");
                throw;
            }
        }

        private void Merge()
        {
            Name ??= Environment.MachineName;
            SelfHealing ??= new SelfHealingConfig {Attempts = -1};
            foreach (var connector in Connectors)
            {
                connector.MergeBootstrapSettings(this);
                connector.MergeSecurityProtocolSettings(this);
                connector.MergeCommitSettings(this);
                connector.MergeErrorToleranceSettings(Errors);
                connector.MergeSelfHealingSettings(SelfHealing);
                connector.MergeTopicSettings();
                connector.MergeProcessorTopicOverrides();
                connector.MergePublishers(Publishers);
                
                if (connector.Publisher is {Enabled: true})
                {
                    connector.Publisher.MergePublisherConfigs(this);
                    connector.Publisher.Enrichers ??= new List<ProcessorConfig>();
                    connector.Publisher.Serializers ??= new ConverterConfig();
                }
            }
        }


        private void Verify()
        {
            if (string.IsNullOrEmpty(BootstrapServers) && Connectors.Any(c => string.IsNullOrEmpty(c.BootstrapServers)))
            {
                throw new ArgumentException("Kafka Brokers configuration property must be specified.");
            }
            

            if (Connectors == null || !Connectors.Any())
            {
                throw new ArgumentException("No connectors configured.");
            }

            var hash = new HashSet<string>();
            
            hash.Clear();
            if (!Connectors.All(c => hash.Add(c.Name) && !string.IsNullOrEmpty(c.Name)))
            {
                throw new ArgumentException("Connector Name configuration property must be specified and must be unique.");
            }

            hash.Clear();
            if (!Plugins.All(p => hash.Add(p.Name) && !string.IsNullOrEmpty(p.Name)))
            {
                throw new ArgumentException("Plug in Name configuration property must be specified and must be unique.");
            }

            foreach (var connector in Connectors)
            {
                if (!Plugins.Select(p => p.Name).Contains(connector.Plugin))
                {
                    throw new ArgumentException(
                        $"Connector: {connector.Plugin} is not associated to any of the available Plugins: [ {string.Join(", ", Plugins.Select(p => p.Name))} ].");
                }
            }
        }
    }
}