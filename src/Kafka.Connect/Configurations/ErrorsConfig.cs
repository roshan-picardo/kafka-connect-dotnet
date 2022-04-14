namespace Kafka.Connect.Configurations
{
    public class ErrorsConfig
    {
        public ErrorTolerance Tolerance { get; init; }
        public string Topic { get; set; }
    }
}