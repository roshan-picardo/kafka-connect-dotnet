using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Plugin.Processors;

public abstract class Processor<T> : IProcessor where T: class
{
    private readonly IConfigurationProvider _configurationProvider;

    protected Processor(IConfigurationProvider configurationProvider)
    {
        _configurationProvider = configurationProvider;
    }
        
    public Task<(bool Skip, ConnectMessage<IDictionary<string, object>> Flattened)> Apply(string connector, ConnectMessage<IDictionary<string, object>> message)
    {
        return Apply(_configurationProvider.GetProcessorSettings<T>(connector, GetType().FullName), message);
    }
        
    protected abstract Task<(bool Skip,  ConnectMessage<IDictionary<string, object>> Flattened)> Apply(T settings, ConnectMessage<IDictionary<string, object>> message);
}