using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Kafka.Connect.Models;

public class WorkerContext
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    public string Name { get; internal set; }
    public string Status => Worker.IsPaused ? "Paused" : Worker.IsStopped ? "Stopped" : "Running";
    public TimeSpan Uptime => _stopwatch.Elapsed;
    public IList<ConnectorContext> Connectors { get; } = new List<ConnectorContext>();
    public bool IsStopped => Worker.IsStopped && (Connectors?.All(c => c.IsStopped) ?? true);
    public IWorker Worker { get; internal set; }
    public RestartContext RestartContext { get; internal set; }
}