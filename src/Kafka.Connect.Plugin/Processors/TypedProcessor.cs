using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Options;

namespace Kafka.Connect.Plugin.Processors
{
    public abstract class TypedProcessor<TOutput, TInput, TSettings> : Processor<TSettings> where TSettings: class
    {
        private readonly IRecordFlattener _recordFlattener;

        protected TypedProcessor(IRecordFlattener recordFlattener, IOptions<List<ConnectorConfig<TSettings>>> options,
            IOptions<ConnectorConfig<TSettings>> shared) : base(options, shared)
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