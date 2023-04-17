using System;
using System.Diagnostics;
using Kafka.Connect.Plugin.Models;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Utilities
{
    public static class LoggerExtensions
    {
        public static decimal EndTiming(this Stopwatch stopwatch)
        {
            stopwatch.Stop();
            return decimal.Round(decimal.Divide(stopwatch.ElapsedTicks, TimeSpan.TicksPerMillisecond * 100), 2);
        }

        public static void LogDocument(this Models.SinkRecord sinkRecord)
        {
            Serilog.Log.ForContext<Models.SinkRecord>().Debug("{@Document}", new JObject
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