using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Kafka.Connect.Models
{
    public class WorkerContext
    {
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        public string Name { get; set; }
        public Status Status { get; set; }
        public TimeSpan Uptime => _stopwatch.Elapsed;
        public IList<ConnectorContext> Connectors { get; init; }
        public CancellationTokenSource Token { get; set; }
    }
}