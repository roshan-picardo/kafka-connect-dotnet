using System.Collections.Generic;
using Confluent.SchemaRegistry;

namespace Kafka.Connect.Schemas
{
    public class ProducerCachedSchemaRegistryClient : CachedSchemaRegistryClient
    {
        public ProducerCachedSchemaRegistryClient(IEnumerable<KeyValuePair<string, string>> config) : base(config)
        {
        }
    }
}