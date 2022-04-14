using System.Collections.Generic;

namespace Kafka.Connect.Mongodb.Models
{
    public class WriteStrategy
    {
        public string Name { get; set; }
        public string Selector { get; set; }
        public IDictionary<string, string> Overrides { get; set; }
        public bool IsWriteOrdered { get; set; } 
    }
}