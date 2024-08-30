namespace Kafka.Connect.Configurations;

public class ResiliencyConfig
{
    public BatchConfig Batches { get; set; }
    public RetryConfig Retries { get; set; }
    public ErrorsConfig Errors { get; set; }
    public EofConfig Eof { get; set; }
}

public class BatchConfig
{
    public int Size { get; init; }
    public int Parallelism { get; init; }
    public int Interval { get; init; }
}

public class RetryConfig
{
    public int Attempts { get; set; }
    public int Interval { get; set; }
}

public class ErrorsConfig
{
    public ErrorTolerance Tolerance { get; init; }
    public string Topic { get; init; }
}

public enum ErrorTolerance
{
    None, // No errors are tolerated
    Data, // only data errors are tolerated 
    All // all errors are tolerated
}

public class EofConfig
{
    public bool Enabled { get; init; }
    public string Topic { get; init; }
}