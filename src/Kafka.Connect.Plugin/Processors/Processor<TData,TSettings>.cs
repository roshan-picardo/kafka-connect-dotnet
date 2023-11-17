using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Plugin.Processors
{
    public abstract class Processor<TData, TSettings> : Processor<TSettings> where TSettings: class
    {

        protected Processor(IConfigurationProvider configurationProvider) : base(configurationProvider)
        {
        }

        protected override async Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, TSettings settings)
        {
            var (skipNext, data) = await Apply(flattened.ToObject<TData>(), settings);
            return (skipNext, data.FromObject());
        }

        protected abstract Task<(bool SkipNext, TData Data)> Apply(TData input, TSettings settings);
    }
}