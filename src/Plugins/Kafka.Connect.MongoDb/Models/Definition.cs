using System.Collections.Generic;

namespace Kafka.Connect.MongoDb.Models
{
    public class Definition
    {
        public string Collection { get; set; } 
        public bool? Create { get; set; } 
        public IEnumerable<Index> Indexes { get; set; } 
    }
}