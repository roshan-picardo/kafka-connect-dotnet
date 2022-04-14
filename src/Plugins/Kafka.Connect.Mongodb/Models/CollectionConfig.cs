using System.Collections.Generic;

namespace Kafka.Connect.Mongodb.Models
{
    public class CollectionConfig
    {
        public string Name { get; set; }
        public IDictionary<string, string[]> Overrides { get; set; }
    }
}