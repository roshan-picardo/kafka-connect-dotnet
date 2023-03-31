using System;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Models;

namespace Kafka.Connect.Utilities
{
    public static class HeadersExtensions
    {
        private static dynamic SetTime(this Headers headers, Action<LogTimestamp> setTime)
        {
            LogTimestamp logTimestamp;
            var timestamp = headers.SingleOrDefault(h => h.Key == "_logTimestamp");
            if (timestamp != null)
            {
                headers.Remove("_logTimestamp");
                logTimestamp = ByteConvert.Deserialize<LogTimestamp>(timestamp.GetValueBytes());
            }
            else
            {
                logTimestamp = new LogTimestamp();
            }

            setTime(logTimestamp);
            headers.Add("_logTimestamp", ByteConvert.Serialize(logTimestamp));
            return logTimestamp;
        }

        public static void StartTiming(this Headers headers, long? millis = null)
        {
            headers.SetTime(t =>
            {
                t.Created = millis ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                t.Consumed = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            });
        }

        public static dynamic EndTiming(this Headers headers, int batchSize)
        {
            var logTimestamp = headers.SetTime(t =>
            {
                t.Committed = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                t.BatchSize = batchSize;
            });
            return new
            {
                Lag = logTimestamp.Lag.ToString(@"dd\.hh\:mm\:ss\.fff"),
                Total = logTimestamp.Total.ToString(@"dd\.hh\:mm\:ss\.fff"),
                logTimestamp.Duration,
                Batch = new { Size = batchSize, Total = logTimestamp.Batch }
            };
        }
    }
}