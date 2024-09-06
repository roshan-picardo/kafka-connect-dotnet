namespace Kafka.Connect.Configurations;

public class FailOverConfig
{
    private readonly int _initialDelayMs = 60000;
    private readonly int _failureThreshold = 20;
    private readonly int _restartDelayMs = 10000;
    private readonly int _periodicDelayMs = 10000;
    public bool Disabled { get; init; }

    public int InitialDelayMs
    {
        get => _initialDelayMs <= 0 ? 60000 : _initialDelayMs;
        init => _initialDelayMs = value;
    }

    public int PeriodicDelayMs
    {
        get => _periodicDelayMs <= 0 ? 10000 : _periodicDelayMs;
        init => _periodicDelayMs = value;
    }

    public int FailureThreshold
    {
        get => _failureThreshold <= 0 ? 20 : _failureThreshold;
        init => _failureThreshold = value;
    }

    public int RestartDelayMs 
    {
        get => _restartDelayMs <= 0 ? 10000 : _restartDelayMs;
        init => _restartDelayMs = value;
    }
}