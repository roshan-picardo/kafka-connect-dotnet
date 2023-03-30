using System;

namespace Kafka.Connect.Plugin.Logging
{
    public interface ILogger<T> where T : class
    {
        void Trace(string message, object data = null, Exception exception = null);
        void Debug(string message, object data = null, Exception exception = null);
        void Info(string message, object data = null, Exception exception = null);
        void Warning(string message, object data = null, Exception exception = null);
        void Error(string message, object data = null, Exception exception = null);
        void Critical(string message, object data = null, Exception exception = null);
        void None(string message, object data = null, Exception exception = null);
        TimedLog Track(string message);
    }
}