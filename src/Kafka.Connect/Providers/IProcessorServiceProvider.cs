using System.Collections.Generic;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Processors;

namespace Kafka.Connect.Providers;

public interface IProcessorServiceProvider
{
    IEnumerable<IProcessor> GetProcessors();
    IMessageConverter GetMessageConverter(string typeName);
}
