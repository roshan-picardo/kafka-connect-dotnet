using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Plugin.Processors
{
    public abstract class Processor<T> : IProcessor where T: class
    {
        private readonly IConfigurationProvider _configurationProvider;

        protected Processor(IConfigurationProvider configurationProvider)
        {
            _configurationProvider = configurationProvider;
        }
        public Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, string connector)
        {
            return Apply(flattened, _configurationProvider.GetProcessorSettings<T>(connector, GetType().FullName));
        }

        protected abstract Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, T settings);
        
        public bool IsOfType(string type)
        {
            return GetType().FullName == type;
        }
    }
}