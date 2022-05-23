using System.Collections;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Plugin.Logging
{
    public class OperationLogAttribute : LogAttribute
    {
        public OperationLogAttribute(LogLevel logLevel, string message) : base(message, null)
        {
            LogEventLevel = logLevel;
        }

        public OperationLogAttribute(string message) : base(message, null)
        {
        }

        public OperationLogAttribute(string message, string[] data): base(message, data)
        {
            
        }

        public LogLevel? LogEventLevel { get; }

        public LogLevel StartLogLevel => LogEventLevel ?? LogLevel.Trace;

        public LogLevel EndLogLevel => LogEventLevel ?? LogLevel.Information;
    }
}