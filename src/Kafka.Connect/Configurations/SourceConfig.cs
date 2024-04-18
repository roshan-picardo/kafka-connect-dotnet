namespace Kafka.Connect.Configurations
{
    public class SourceConfig
    {
        public string Plugin { get; set; }
        public string Handler { get; set; }
        public string Topic { get; set; }
        public int BatchSize { get; set; }
        public int TimeOutInMs { get; set; }
        
        public StrategyConfig Strategy { get; set; }
    }

    public class SourceConfig<T> : SourceConfig
    {
        public T Properties { get; set; }
    }
}