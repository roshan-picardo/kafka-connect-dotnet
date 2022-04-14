using System.Collections.Generic;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Serializers;

namespace Kafka.Connect.Providers
{
    public interface IProcessorServiceProvider
    {
        IEnumerable<IProcessor> GetProcessors();
        IDeserializer GetDeserializer(string typeName);
        ISerializer GetSerializer(string typeName);
        IEnumerable<IEnricher> GetEnrichers();
    }
}