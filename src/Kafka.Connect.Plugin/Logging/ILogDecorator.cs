using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Plugin.Logging
{
    public interface ILogDecorator
    {
        T Build<T>(T decorated, ILogger logger);
    }
}