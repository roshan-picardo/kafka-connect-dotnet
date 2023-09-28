using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kafka.Connect.Postgres;

public class DefaultPluginInitializer : PluginInitializer
{
    protected override void AddAdditionalServices(IServiceCollection collection, IConfiguration configuration)
    {
        // placeholder for custom injections
    }
}