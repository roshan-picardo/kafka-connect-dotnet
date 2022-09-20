namespace Kafka.Connect.Plugin.Providers
{
    public interface IConfigurationProvider
    {
        T GetProcessorSettings<T>(string connector, string processor);
    }
}