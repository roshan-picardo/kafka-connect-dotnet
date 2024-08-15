using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Providers;

public interface IConfigurationProvider
{
    T GetProcessorSettings<T>(string connector, string processor);
    T GetLogAttributes<T>(string connector);
    string GetPluginName(string connector);
    T GetPluginConfig<T>(string connector);
    ParallelRetryOptions GetParallelRetryOptions(string connector);
}