namespace Kafka.Connect.Config.Models
{
    public class BatchConfig
    {
        private readonly int? _size = 1;

        public int? Size
        {
            get => _size.GetValueOrDefault() <= 0 ? 1 : _size.GetValueOrDefault();
            init => _size = value;
        }

        public int? Parallelism { get; init; }
    }
}