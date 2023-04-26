namespace Kafka.Connect.MongoDb.Models
{
    public class BatchConfig
    {
        public int? Size { get; set; }
        public int? Parallelism { get; set; }
    }
}