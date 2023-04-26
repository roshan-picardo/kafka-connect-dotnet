using System.Collections.Generic;

namespace Kafka.Connect.MongoDb.Models
{
    public class Index
    {
        public string Name { get; set; } 
        public IEnumerable<Field> Fields { get; set; } 
        public bool Unique { get; set; } 
    }
}