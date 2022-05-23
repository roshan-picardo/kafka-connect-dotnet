using Kafka.Connect.Plugin.Logging;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Logging
{
    public class LogDecorator : ILogDecorator
    {
        public T Build<T>(T decorated, ILogger logger)
        {
            return LogDispatchProxy<T>.Create(decorated, logger);
        }
    }
}