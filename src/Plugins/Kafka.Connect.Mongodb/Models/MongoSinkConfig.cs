using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Exceptions;
using MongoDB.Driver;

namespace Kafka.Connect.Mongodb.Models
{
    public class MongoSinkConfig
    {
        internal string Connector { get; set; }
        public Properties Properties { get; set; } 
        public BatchConfig Batch { get; set; }
        
        public bool HasTopicOverrides()
        {
            if (Properties?.Collection == null)
            {
                throw new ConnectDataException(ErrorCode.InvalidConfig, new MongoConfigurationException("No collection defined to sink the data."));
            }
            return Properties.Collection.Overrides != null && Properties.Collection.Overrides.Any();
        }

        public IEnumerable<(string Collection, string[] Topics)> GetCollectionTopicMaps(IEnumerable<string> topics)
        {
            if (Properties.Collection.Overrides == null || !Properties.Collection.Overrides.Any())
            {
                return topics.Select(r => (r, new[] {Properties.Collection.Name})).ToList();
            }

            var overrides = Properties.Collection.Overrides.SelectMany(m =>
                m.Value.Select(s => new {topic = m.Key, collection = s})).ToList();

            foreach (var topic in topics)
            {
                if (overrides.All(o => o.topic != topic))
                {
                    overrides.Add(new {topic, collection = Properties.Collection.Name});
                }
            }

            return overrides.GroupBy(o => o.collection,
                    (c, group) => new {Collection = c, Topics = group.Select(g => g.topic).ToArray()})
                .Select(s => (s.Collection, s.Topics)).ToList();
        }
    }
}