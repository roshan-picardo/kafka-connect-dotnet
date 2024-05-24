using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using Serilog.Core;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Plugin.Logging
{
    public class ConnectLog : IDisposable
    {
        private readonly ILogger _logger;
        private readonly string _message;
        private readonly Stopwatch _stopwatch;

        public ConnectLog(ILogger logger, string message)
        {
            _logger = logger;
            _message = message;
            _stopwatch = Stopwatch.StartNew();

            _logger.LogTrace("{@Log}", new
            {
                Message = _message,
                Operation = "Started"
            });
        }

        public void Dispose()
        {
            _stopwatch.Stop();
            _logger.LogDebug("{@Log}", new
            {
                Message = _message,
                Operation = "Finished",
                Duration = decimal.Round(decimal.Divide(_stopwatch.ElapsedTicks, TimeSpan.TicksPerMillisecond * 100), 3)
            });
        }

        public static IDisposable Connector(string name) =>
            LogContext.Push(new PropertyEnricher("Connector", name));
        
        public static IDisposable Worker(string name) =>
            LogContext.Push(new PropertyEnricher("Worker", name));
        
        public static IDisposable Leader(string name) =>
            LogContext.Push(new PropertyEnricher("Leader", name));
        
        
        public static IDisposable Command(string name) =>
            LogContext.Push(new PropertyEnricher("Command", name));
        
        public static IDisposable Batch() =>
            LogContext.Push(new PropertyEnricher("Batch", (Guid.NewGuid().GetHashCode() & 0x7FFFFFFF).ToString("0000000000")));
        
        public static IDisposable Task(int taskId) =>
            LogContext.Push(new PropertyEnricher("Task", taskId.ToString("00")));

        public static IDisposable TopicPartitionOffset(string topic, int? partition = null, long? offset = null)
        {
            var properties = new List<PropertyEnricher> { new("Topic", topic) };
            if (partition != null)
            {
                properties.Add(new("Partition", partition.Value));
            }
            if (offset != null)
            {
                properties.Add(new("Offset", offset.Value));
            }

            return LogContext.Push(properties.Cast<ILogEventEnricher>().ToArray());
        }

        public static IDisposable Mongo(string database, string collection) =>
            LogContext.Push(new PropertyEnricher("Database", database), new PropertyEnricher("Collection", collection));
    }
}