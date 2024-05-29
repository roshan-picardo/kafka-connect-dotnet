using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using IConfigurationProvider = Kafka.Connect.Plugin.Providers.IConfigurationProvider;

namespace Kafka.Connect.Plugin
{
    public interface IPluginInitializer
    {
        void AddServices(IServiceCollection collection, IConfiguration configuration, params (string Name, int Tasks)[] connectors);
    }

    public abstract class PluginInitializer : IPluginInitializer
    {
        public void AddServices(
            IServiceCollection collection,
            IConfiguration configuration,
            (string Plugin, IEnumerable<(string Name, int Tasks)> Connectors) pluginConfig)
        {
            throw new System.NotImplementedException();
        }

        public abstract void AddServices(IServiceCollection collection, IConfiguration configuration, params (string Name, int Tasks)[] connectors);
    }
}