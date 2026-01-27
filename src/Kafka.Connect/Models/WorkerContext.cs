using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;

namespace Kafka.Connect.Models;

public class WorkerContext
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    public string Name { get; internal set; }

    public string Status => Worker == null ? "Stopped" :
        Worker.IsPaused ? "Paused" :
        Worker.IsStopped ? "Stopped" : "Running";
    public TimeSpan Uptime => _stopwatch.Elapsed;
    public ConcurrentBag<ConnectorContext> Connectors { get; } = new();
    public bool IsStopped => Worker == null || (Worker.IsStopped && (Connectors?.All(c => c.IsStopped) ?? true));
    public IWorker Worker { get; internal set; }
    public ILeader Leader { get; internal set; }
    public RestartContext RestartContext { get; internal set; }
}