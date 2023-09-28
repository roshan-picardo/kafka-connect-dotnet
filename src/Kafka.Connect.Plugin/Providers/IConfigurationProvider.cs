namespace Kafka.Connect.Plugin.Providers;

public interface IConfigurationProvider
{
    T GetProcessorSettings<T>(string connector, string processor);
    T GetSinkConfigProperties<T>(string connector, string plugin = null);
    T GetLogAttributes<T>(string connector);
    string GetPluginName(string connector);
}