using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Kafka.Connect.Connectors;

namespace Kafka.Connect.Models;

public class ConnectorContext
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    public string Name { get; init; }

    public string Status => Connector == null ? "Stopped" :
        Connector.IsPaused ? "Paused" :
        Connector.IsStopped ? "Stopped" : "Running";
    public TimeSpan Uptime => _stopwatch.Elapsed;
    public IList<TaskContext> Tasks { get; } = new List<TaskContext>();
    public bool IsStopped => Connector == null || (Connector.IsStopped && (Tasks?.All(t => t.IsStopped) ?? true));
    public IConnector Connector { get; internal set; }
    public RestartContext RestartContext { get; set; }
}