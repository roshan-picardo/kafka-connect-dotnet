using System;
using System.Diagnostics;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Serilog;

namespace Kafka.Connect.Utilities
{
    public static class LoggerExtensions
    {
        public static void LogOperationCancelled<T>(this ILogger<T> logger, OperationCanceledException oce)
        {
            if (oce.CancellationToken.IsCancellationRequested)
            {
                logger.LogInformation("Worker shutdown initiated. Connector task will be shutdown.");
            }
            else
            {
                logger.LogError(oce, "Unexpected error while shutting down the Worker.");
            }
        }

        public static decimal EndTiming(this Stopwatch stopwatch)
        {
            stopwatch.Stop();
            return decimal.Round(decimal.Divide(stopwatch.ElapsedTicks, TimeSpan.TicksPerMillisecond * 100), 2);
        }

        public static void LogDocument(this SinkRecord sinkRecord)
        {
            Log.ForContext<SinkRecord>().Debug("{@document}", new JObject
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
    }
}