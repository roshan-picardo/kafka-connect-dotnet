using System.Threading.Tasks;
using System.Timers;
using Kafka.Connect.Configurations;

namespace Kafka.Connect.Models;

public class RestartContext
{
    private readonly RestartsConfig _config;
    private readonly RestartsLevel _restartsLevel;
    private int _remainingAttempts;
    private readonly Timer _timer;

    public RestartContext(RestartsConfig config, RestartsLevel restartsLevel)
    {
        _config = config;
        _restartsLevel = restartsLevel;
        _remainingAttempts = config.Attempts;
        _timer = new Timer(config.RetryWaitTimeMs);
        _timer.Elapsed += (_, _) =>
        {
            if (config.Attempts <= 0) return;
            _remainingAttempts = config.Attempts;
            _timer.Stop();
        };
    }

    public async Task<bool> Retry()
    {
        _timer.Stop();
        if (!_config.EnabledFor.HasFlag(_restartsLevel)) return false;
        switch (_config.Attempts)
        {
            case < 0:
                await Task.Delay(_config.PeriodicDelayMs);
                return true;
            case 0:
                return false;
            case > 0 when _remainingAttempts-- > 0:
                await Task.Delay(_config.PeriodicDelayMs);
                _timer.Start();
                return true;
        }
        return false;
    }
}