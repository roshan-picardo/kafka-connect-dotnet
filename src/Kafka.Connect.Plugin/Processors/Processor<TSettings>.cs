using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Plugin.Processors
{
    public abstract class Processor<TSettings> : IProcessor where TSettings: class
    {
        private readonly IConfigurationProvider _configurationProvider;

        protected Processor(IConfigurationProvider configurationProvider)
        {
            _configurationProvider = configurationProvider;
        }
        
        public Task<ConnectMessage<IDictionary<string, object>>> Apply(string connector, ConnectMessage<IDictionary<string, object>> message)
        {
            return Apply(_configurationProvider.GetProcessorSettings<TSettings>(connector, GetType().FullName),
                message);
        }
        
        protected abstract Task<ConnectMessage<IDictionary<string, object>>> Apply(TSettings settings, ConnectMessage<IDictionary<string, object>> message);
        
        public bool IsOfType(string type)
        {
            return GetType().FullName == type;
        }
    }
}