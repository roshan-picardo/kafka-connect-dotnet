using System;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Plugin.Logging
{
    public class Logger<T> : ILogger<T> where T : class
    {
        private readonly Microsoft.Extensions.Logging.ILogger<T> _logger;

        public Logger(Microsoft.Extensions.Logging.ILogger<T> logger)
        {
            _logger = logger;
        }

        private void Log(LogLevel level, string message, Exception exception = null, object data = null)
        {
            if (data is Exception exp)
            {
                data = null;
            }
            else
            {
                exp = exception;
            }

            switch (exp)
            {
                case null when data == null:
                    _logger.Log(level, "{@Log}", new { Message = message });
                    break;
                case null:
                    _logger.Log(level, "{@Log}", new { Message = message, Data = data });
                    break;
                default:
                {
                    if (data == null)
                    {
                        _logger.Log(level, exp, "{@Log}", new { Message = message });
                    }
                    else
                    {
                        _logger.Log(level, exp, "{@Log}", new { Message = message, Data = data });
                    }

                    break;
                }
            }
        }

        public void Trace(string message, object data = null, Exception exception = null)
        {
            Log(LogLevel.Trace, message, exception, data);
        }

        public void Debug(string message, object data = null, Exception exception = null)
        {
            Log(LogLevel.Debug, message, exception, data);
        }

        public void Info(string message, object data = null, Exception exception = null)
        {
            Log(LogLevel.Information, message, exception, data);
        }

        public void Warning(string message, object data = null, Exception exception = null)
        {
            Log(LogLevel.Warning, message, exception, data);
        }

        public void Error(string message, object data = null, Exception exception = null)
        {
            Log(LogLevel.Error, message, exception, data);
        }

        public void Critical(string message, object data = null, Exception exception = null)
        {
            Log(LogLevel.Critical, message, exception, data);
        }

        public void None(string message, object data = null, Exception exception = null)
        {
            Log(LogLevel.None, message, exception, data);
        }

        public TimedLog Track(string message)
        {
            return new TimedLog(_logger, message);
        }
    }
}

