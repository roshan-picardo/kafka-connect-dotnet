using Kafka.Connect.Plugin;

namespace Kafka.Connect.Providers
{
    public interface ISinkHandlerProvider
    {
        ISinkHandler GetSinkHandler(string connector);
    }
}