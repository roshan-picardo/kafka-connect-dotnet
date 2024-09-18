using System.Collections.Generic;

namespace Kafka.Connect.Configurations;

public class FaultToleranceConfig
{
    public BatchConfig Batches { get; init; }
    public RetryConfig Retries { get; init; }
    public ErrorsConfig Errors { get; init; }
    public EofConfig Eof { get; init; }
}

public class BatchConfig
{
    public int Size { get; init; }
    public int Parallelism { get; init; }
    public int Interval { get; init; }
}

public class RetryConfig
{
    public int Attempts { get; init; }
    public int Interval { get; init; }
}

public class ErrorsConfig
{
    public ErrorTolerance Tolerance { get; init; }
    public List<string> Exceptions { get; init; }
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