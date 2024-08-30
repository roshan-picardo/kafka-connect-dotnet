namespace Kafka.Connect.Configurations;

public class ResiliencyConfig
{
    public BatchConfig Batches { get; set; }
    public RetryConfig Retries { get; set; }
    public ErrorsConfig Errors { get; set; }
    public EofConfig Eof { get; set; }
}
