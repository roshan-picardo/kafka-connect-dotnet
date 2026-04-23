using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using Xunit;

namespace UnitTests.Kafka.Connect.Plugin.Logging;

public class LoggerTests
{
    [Fact]
    public void PublicLogMethods_EmitExpectedLevelsAndPayloadShapes()
    {
        var logger = new TestLogger<LoggerTests>();
        var sinkLogger = new TestLogger<ConnectLog>();
        var sut = new global::Kafka.Connect.Plugin.Logging.Logger<LoggerTests>(logger, sinkLogger);
        var explicitException = new InvalidOperationException("boom");

        sut.Trace("trace");
        sut.Debug("debug", new { Value = 1 });
        sut.Info("info", new Exception("data-exception"));
        sut.Warning("warning", new { Value = 2 }, explicitException);
        sut.Error("error", null, explicitException);
        sut.Critical("critical", new { Value = 3 }, explicitException);
        sut.None("none", new { Value = 4 });

        Assert.Equal([LogLevel.Trace, LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error, LogLevel.Critical, LogLevel.None],
            logger.Entries.Select(entry => entry.Level).ToArray());

        Assert.Null(logger.Entries[0].Exception);
        Assert.Null(GetNestedProperty(logger.Entries[0].State, "@Log", "Data"));
        Assert.Equal(explicitException, logger.Entries[3].Exception);
        Assert.Equal("warning", GetNestedProperty(logger.Entries[3].State, "@Log", "Message"));
        Assert.Equal("data-exception", ((Exception)logger.Entries[2].Exception).Message);
    }

    [Fact]
    public void RecordDocumentHealthAndTrack_LogToExpectedTargets()
    {
        var logger = new TestLogger<LoggerTests>();
        var sinkLogger = new TestLogger<ConnectLog>();
        var sut = new global::Kafka.Connect.Plugin.Logging.Logger<LoggerTests>(logger, sinkLogger);
        var exception = new InvalidOperationException("record-boom");

        sut.Record(new { Id = 1 }, exception);
        sut.Document(new ConnectMessage<JsonNode>
        {
            Key = JsonNode.Parse("""{ "id": 1 }"""),
            Value = JsonNode.Parse("""{ "name": "Ada" }""")
        });
        sut.Health(new { Status = "ok" });
        using (sut.Track("sync"))
        {
        }

        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Information && GetKey(entry.State) == "@Record");
        Assert.Contains(sinkLogger.Entries, entry => entry.Level == LogLevel.Debug && GetKey(entry.State) == "@Document");
        Assert.Contains(sinkLogger.Entries, entry => entry.Level == LogLevel.Information && GetKey(entry.State) == "@Health");
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Trace && Equals(GetNestedProperty(entry.State, "@Log", "Operation"), "Started"));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Debug && Equals(GetNestedProperty(entry.State, "@Log", "Operation"), "Finished"));
    }

    private static string GetKey(object state) =>
        ((IEnumerable<KeyValuePair<string, object>>)state).First().Key;

    private static object GetNestedProperty(object state, string key, string propertyName)
    {
        var outer = ((IEnumerable<KeyValuePair<string, object>>)state).First(pair => pair.Key == key).Value;
        return outer.GetType().GetProperty(propertyName)?.GetValue(outer);
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