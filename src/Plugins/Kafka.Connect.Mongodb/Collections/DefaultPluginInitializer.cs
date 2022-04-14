using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kafka.Connect.Mongodb.Collections
{
    public class DefaultPluginInitializer : PluginInitializer
    {
        protected override void AddMoreServices(IServiceCollection collection, IConfiguration configuration, string plugin)
        {
        }
    }
}