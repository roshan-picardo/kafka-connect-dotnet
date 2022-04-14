using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Plugin.Logging
{
    public static class LoggerExtensions
    {
        public static TimedLogBuilder Timed(this ILogger logger, string message)
        {
            return new TimedLogBuilder(logger, message);
        }
    }
}