using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Plugin.Logging
{
    public class Logger<T> : ILogger<T> where T : class
    {
        private readonly Microsoft.Extensions.Logging.ILogger<T> _logger;
        private readonly Microsoft.Extensions.Logging.ILogger<SinkLog> _sinkLogger;
        private readonly IEnumerable<ILogRecord> _logRecords;

        public Logger(Microsoft.Extensions.Logging.ILogger<T> logger,
            Microsoft.Extensions.Logging.ILogger<SinkLog> sinkLogger,
            IEnumerable<ILogRecord> logRecords)
        {
            _logger = logger;
            _sinkLogger = sinkLogger;
            _logRecords = logRecords;
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

        public void Record(SinkRecord record, string provider, string connector, int batch)
        {
            var attributes = _logRecords.SingleOrDefault(l => l.GetType().FullName == provider)?.Enrich(record, connector);
            _sinkLogger.Log(LogLevel.Information, "{@Record}", new
            {
                record.Status, 
                Timers = record.EndTiming(batch),
                Attributes = attributes
            });
        }

        public void Document(object document)
        {
            _sinkLogger.Log(LogLevel.Trace, "{@Document}", document);
        }

        public void Health(object health)
        {
            _sinkLogger.Log(LogLevel.Information, "{@Health}", health);
        }

        public SinkLog Track(string message)
        {
            return new SinkLog(_logger, message);
        }
    }
}

