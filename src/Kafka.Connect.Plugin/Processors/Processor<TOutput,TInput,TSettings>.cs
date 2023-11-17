using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Plugin.Processors
{
    public abstract class Processor<TOutput, TInput, TSettings> : Processor<TSettings> where TSettings: class
    {
        protected Processor(IConfigurationProvider configurationProvider) : base(configurationProvider)
        {
        }

        protected override async Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, TSettings settings)
        {
            var (skipNext, data) = await Apply(flattened.ToObject<TInput>(), settings);
            return (skipNext, data.FromObject());
        }

        protected abstract Task<(bool SkipNext, TOutput Data)> Apply(TInput input, TSettings settings);
    }
}