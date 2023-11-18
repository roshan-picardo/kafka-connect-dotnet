using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Plugin.Processors
{
    public abstract class Processor<TData, TSettings> : Processor<TSettings> where TSettings: class
    {

        protected Processor(IConfigurationProvider configurationProvider) : base(configurationProvider)
        {
        }

        protected abstract Task<(bool SkipNext, TData Data)> Apply(TData input, TSettings settings);
        protected override Task<ConnectMessage<IDictionary<string, object>>> Apply(TSettings settings, ConnectMessage<IDictionary<string, object>> message)
        {
            throw new System.NotImplementedException();
        }
    }
}