namespace Kafka.Connect.Mongodb.Models
{
    public class BatchConfig
    {
        public int? Size { get; set; }
        public int? Parallelism { get; set; }
    }
}