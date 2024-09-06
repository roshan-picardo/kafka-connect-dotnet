namespace Kafka.Connect.Configurations;

public class HealthCheckConfig
{
    private readonly int _timeout = 60000;
    private readonly int _interval = 60000;
    public bool Disabled { get; init; }
    public int Timeout 
    {
        get => _timeout <= 0 ? 60000 : _timeout;
        init => _timeout = value;
    }
        
    public int Interval 
    {
        get => _interval <= 0 ? 60000 : +_interval;
        init => _interval = value;
    }
}