using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Kafka.Connect.Models
{
    public class TaskContext
    {
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        public int Id { get; init; }
        public Status Status { get; set; }
        public TimeSpan Uptime => _stopwatch.Elapsed;
        public IList<(string, int)> TopicPartitions { get; init; }
        public BatchPollContext BatchContext { get; set; }
        public CancellationTokenSource Token { get; set; }
    }
}