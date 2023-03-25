using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kafka.Connect.Plugin
{
    public interface IPluginInitializer
    {
        void AddServices(IServiceCollection collection, IConfiguration configuration, string plugin, IEnumerable<string> connectors);
    }
}