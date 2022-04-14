using System.Collections.Generic;
using Confluent.SchemaRegistry;

namespace Kafka.Connect.Schemas
{
    public class ConsumerCachedSchemaRegistryClient : CachedSchemaRegistryClient
    {
        public ConsumerCachedSchemaRegistryClient(IEnumerable<KeyValuePair<string, string>> config) : base(config)
        {
        }
    }
}