using System;
using System.Diagnostics;

namespace Kafka.Connect.Utilities
{
    public static class LoggerExtensions
    {
        public static decimal EndTiming(this Stopwatch stopwatch)
        {
            stopwatch.Stop();
            return decimal.Round(decimal.Divide(stopwatch.ElapsedTicks, TimeSpan.TicksPerMillisecond * 100), 2);
        }
    }
}