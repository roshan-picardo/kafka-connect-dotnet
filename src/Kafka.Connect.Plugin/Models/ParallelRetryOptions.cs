namespace Kafka.Connect.Plugin.Models;

public class ParallelRetryOptions
{
    public int DegreeOfParallelism { get; init; }
    public int Attempts { get; init; }
    public int TimeoutMs { get; init; }
    public bool ErrorTolerated { get; init; }
}
