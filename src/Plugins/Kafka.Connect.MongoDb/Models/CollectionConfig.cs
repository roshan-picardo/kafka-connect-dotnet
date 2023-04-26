using System.Collections.Generic;

namespace Kafka.Connect.MongoDb.Models
{
    public class CollectionConfig
    {
        public string Name { get; set; }
        public IDictionary<string, string[]> Overrides { get; set; }
    }
}