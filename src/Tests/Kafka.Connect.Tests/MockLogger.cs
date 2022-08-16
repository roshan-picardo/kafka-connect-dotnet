using System;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Tests
{
    public abstract class MockLogger<T> : ILogger<T>
    {
        void ILogger.Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception,
            Func<TState, Exception, string> formatter)
            => Log(logLevel, formatter(state, exception), exception);

        public abstract void Log(LogLevel logLevel, object state, Exception exception = null);

        public virtual bool IsEnabled(LogLevel logLevel) => true;

        public abstract IDisposable BeginScope<TState>(TState state);
    }

    public abstract class MockLogger : ILogger
    {
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception,
            Func<TState, Exception, string> formatter)
            => Log(logLevel, formatter(state, exception), exception);

        public abstract void Log(LogLevel logLevel, object state, Exception exception = null);

        public bool IsEnabled(LogLevel logLevel) => true;

        public abstract IDisposable BeginScope<TState>(TState state);
    }
}