using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Options;

namespace Kafka.Connect.Plugin.Processors
{
    public abstract class Processor<TSettings> : IProcessor where TSettings: class
    {
        private readonly IOptions<List<ConnectorConfig<TSettings>>> _options;
        private readonly IOptions<ConnectorConfig<TSettings>> _shared;

        public Processor(IOptions<List<ConnectorConfig<TSettings>>> options, IOptions<ConnectorConfig<TSettings>> shared)
        {
            _options = options;
            _shared = shared;
        }
        public Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, string connector)
        {
            return Apply(flattened, GetSettings(connector));
        }

        protected abstract Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, TSettings settings);
        
        public bool IsOfType(string type)
        {
            return GetType().FullName == type;
        }

        private TSettings GetSettings(string connector)
        {
            return _options?.Value?.SingleOrDefault(c => c.Name == connector)?.Processors
                       ?.SingleOrDefault(p => IsOfType(p.Name))?.Settings
                   ?? _shared?.Value?.Processors?.SingleOrDefault(p => IsOfType(p.Name))?.Settings;
        }
    }
}