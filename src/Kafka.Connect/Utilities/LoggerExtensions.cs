using System;
using System.Diagnostics;
using System.Linq;
using Kafka.Connect.Logging;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Serilog;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace Kafka.Connect.Utilities
{
    public static class LoggerExtensions
    {
        public static decimal EndTiming(this Stopwatch stopwatch)
        {
            stopwatch.Stop();
            return decimal.Round(decimal.Divide(stopwatch.ElapsedTicks, TimeSpan.TicksPerMillisecond * 100), 2);
        }

        public static void LogDocument(this SinkRecord sinkRecord)
        {
            Serilog.Log.ForContext<SinkRecord>().Debug("{@Document}", new JObject
            {
                {
                    "Record", new JObject
                    {
                        {"Key", sinkRecord.Key},
                        {"Value", sinkRecord.Value}
                    }
                },
                {"Status", SinkStatus.Document.ToString()},
            });
        }

        public static OperationLog Begin(this ILogger logger, string message, string[] data)
        {
            return new OperationLog(logger, message, data);
        }

        public static void Write(this ILogger logger, LogLevel level, string message, object data)
        {
            Write(logger, level, message, null, data);
        }
        public static void Write(this ILogger logger, LogLevel level, string message, Exception exception = null, object data = null)
        {
            switch (exception)
            {
                case null when data == null:
                    logger.Log(level, "{@Log}", new { Message = message});
                    break;
                case null:
                    logger.Log(level, "{@Log}", new { Message = message, Data = data});
                    break;
                default:
                {
                    if (data == null)
                    {
                        logger.Log(level, "{@Log}", new { Message = message, Execption = exception});
                    }
                    else
                    {
                        logger.Log(level,  exception, "{@Log}", new { Message = message, Data = data});
                    }

                    break;
                }
            }
        }
    }
}