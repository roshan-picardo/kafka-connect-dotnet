namespace Kafka.Connect.Plugin.Models
{
    public class ProcessorConfig<T>
    {
        public string Name { get; set; }
        public T Settings { get; set; }
    }
}