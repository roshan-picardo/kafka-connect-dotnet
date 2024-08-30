namespace Kafka.Connect.Configurations;

public class BatchConfig
{
    public int Size { get; set; }
    public int Parallelism { get; set; }
    public int Timeout { get; set; }
}

public class BatchConfigOld
{
    private readonly int _size = 100;
    private readonly int _parallelism = 10;
    private readonly int _timeout = 60000;

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

    public int TimeoutInMs
    {
        get => _timeout <= 0 ? 60000 : _timeout;
        init => _timeout = value;
    }

    public EofConfig EofSignal { get; init; }
        
    public RetryConfigOld Retries { get; init; }
}