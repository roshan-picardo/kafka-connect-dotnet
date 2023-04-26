using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kafka.Connect.MongoDb.Collections
{
    public class DefaultPluginInitializer : PluginInitializer
    {
        protected override void AddAdditionalServices(IServiceCollection collection, IConfiguration configuration)
        {
        }
    }
}