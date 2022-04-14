namespace Kafka.Connect.Configurations
{
    public class BatchConfig
    {
        private readonly int _size = 100;
        private readonly int _parallelism = 10;

        public int Size
        {
            get => _size <= 0 ? 100 : _size;
            init => _size = value;
        }

        public int Parallelism
        {
            get => _parallelism <= 0 ? 10 : _parallelism;
            init => _parallelism = value;
        }
    }
}