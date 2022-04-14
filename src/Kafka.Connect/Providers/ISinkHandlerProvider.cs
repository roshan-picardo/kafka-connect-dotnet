using Kafka.Connect.Plugin;

namespace Kafka.Connect.Providers
{
    public interface ISinkHandlerProvider
    {
        ISinkHandler GetSinkHandler(string plugin, string handler);
        ISinkHandler GetSinkHandler(string connector);
    }
}