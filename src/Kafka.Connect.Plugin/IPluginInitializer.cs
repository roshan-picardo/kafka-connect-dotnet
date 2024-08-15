using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kafka.Connect.Plugin
{
    public interface IPluginInitializer
    {
        void AddServices(IServiceCollection collection, IConfiguration configuration, params (string Name, int Tasks)[] connectors);
    }
}