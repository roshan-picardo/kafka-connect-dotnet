namespace Kafka.Connect.Mongodb.Models
{
    public class SinkConfig<T>
    {
        public string Name { get; set; }
        public string Plugin { get; set; }
        public T Sink { get; set; }
    }
}