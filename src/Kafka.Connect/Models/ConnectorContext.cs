using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Kafka.Connect.Models
{
    public class ConnectorContext
    {
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        public string Name { get; init; }
        public Status Status { get; set; }
        public TimeSpan Uptime => _stopwatch.Elapsed;
        public IList<TaskContext> Tasks { get; init; }
    }
}