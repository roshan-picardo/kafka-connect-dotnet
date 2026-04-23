using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

namespace UnitTests.Kafka.Connect.Plugin.Logging;

public class ConnectLogTests
{
    [Fact]
    public void ConstructorAndDispose_LogStartedAndFinishedOperations()
    {
        var logger = new TestLogger<ConnectLog>();

        using (new ConnectLog(logger, "sync orders"))
        {
        }

        Assert.Equal(2, logger.Entries.Count);
        Assert.Equal(LogLevel.Trace, logger.Entries[0].Level);
        Assert.Equal("Started", GetNestedProperty(logger.Entries[0].State, "@Log", "Operation"));
        Assert.Equal(LogLevel.Debug, logger.Entries[1].Level);
        Assert.Equal("Finished", GetNestedProperty(logger.Entries[1].State, "@Log", "Operation"));
        Assert.NotNull(GetNestedProperty(logger.Entries[1].State, "@Log", "Duration"));
    }

    [Fact]
    public void StaticScopeFactories_ReturnDisposableScopes()
    {
        using var connector = ConnectLog.Connector("orders");
        using var worker = ConnectLog.Worker("worker-a");
        using var leader = ConnectLog.Leader("leader-a");
        using var command = ConnectLog.Command("sync");
        using var batch = ConnectLog.Batch();
        using var task = ConnectLog.Task(4);
        using var topic = ConnectLog.TopicPartitionOffset("orders", 2, 10);
        using var topicOnly = ConnectLog.TopicPartitionOffset("orders");
        using var mongo = ConnectLog.Mongo("db", "collection");

        Assert.NotNull(connector);
        Assert.NotNull(worker);
        Assert.NotNull(leader);
        Assert.NotNull(command);
        Assert.NotNull(batch);
        Assert.NotNull(task);
        Assert.NotNull(topic);
        Assert.NotNull(topicOnly);
        Assert.NotNull(mongo);
    }

    private static object GetNestedProperty(object state, string key, string propertyName)
    {
        var outer = ((IEnumerable<KeyValuePair<string, object>>)state).First(pair => pair.Key == key).Value;
        return outer.GetType().GetProperty(propertyName)!.GetValue(outer);
    }

    private sealed class TestLogger<T> : Microsoft.Extensions.Logging.ILogger<T>
    {
        public List<LogEntry> Entries { get; } = [];

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception,
            Func<TState, Exception, string> formatter)
        {
            Entries.Add(new LogEntry(logLevel, state, exception));
        }
    }

    private sealed record LogEntry(LogLevel Level, object State, Exception Exception);

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}