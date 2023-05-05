using System;
using System.Collections.Generic;
using System.Diagnostics;
using Kafka.Connect.Connectors;

namespace Kafka.Connect.Models;

public class TaskContext
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    public int Id { get; init; }

    public string Status =>
        Task == null ? "Stopped" : Task.IsPaused ? "Paused" : Task.IsStopped ? "Stopped" : "Running";
    public TimeSpan Uptime => _stopwatch.Elapsed;
    public IList<(string, int)> TopicPartitions { get; init; } = new List<(string, int)>();
    public BatchPollContext BatchContext { get; set; }
    public bool IsStopped => Task == null || Task.IsStopped;
    public ISinkTask Task { get; internal set; }
    public RestartContext RestartContext { get; set; }
}