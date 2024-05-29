using System.Collections.Generic;

namespace Kafka.Connect.Plugin.Providers;

public interface IConfigurationProvider
{
    T GetProcessorSettings<T>(string connector, string processor);
    T GetLogAttributes<T>(string connector);
    string GetPluginName(string connector);
    T GetPluginConfig<T>(string connector);
    IList<(string Name, int Tasks)> GetConnectorsByPlugin(string plugin);
}