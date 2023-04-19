using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Plugin.Processors
{
    public abstract class Processor<TOutput, TInput, TSettings> : Processor<TSettings> where TSettings: class
    {
        private readonly IRecordFlattener _recordFlattener;

        protected Processor(IRecordFlattener recordFlattener, IConfigurationProvider configurationProvider) : base(configurationProvider)
        {
            _recordFlattener = recordFlattener;
        }

        protected override async Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, TSettings settings)
        {
            var (skipNext, data) = await Apply(_recordFlattener.ToObject<TInput>(flattened), settings);
            return (skipNext, _recordFlattener.Flatten(data));
        }

        protected abstract Task<(bool SkipNext, TOutput Data)> Apply(TInput input, TSettings settings);
    }
}