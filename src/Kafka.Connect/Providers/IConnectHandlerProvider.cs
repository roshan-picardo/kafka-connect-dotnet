using Kafka.Connect.Plugin;

namespace Kafka.Connect.Providers
{
    public interface IConnectHandlerProvider
    {
        ISinkHandler GetSinkHandler(string connector);
        ISourceHandler GetSourceHandler(string connector);
    }
}