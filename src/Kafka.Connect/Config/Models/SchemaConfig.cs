using System.Collections.Generic;

namespace Kafka.Connect.Config.Models
{
    public class SchemaConfig
    {
        public IDictionary<string, int?> Ids { get; set; }
        public IDictionary<string, string> Subjects { get; set; }
    }
}