using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Kafka.Connect.Logging;
using Kafka.Connect.Plugin;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Logging
{
    public class TimedLogTests
    {
        private readonly ILogger _logger;
        private OperationLog _timedLog;

        public TimedLogTests()
        {
            _logger = Substitute.For<MockLogger>();
        }

        [Theory]
        [InlineData(null, null)]
        [InlineData("", new string [0])]
        [InlineData("Some Message", null)]
        [InlineData("Some Message", new [] { "with", "data"})]
        public void Constructor_Tests(string message, string[] data)
        {
            var logData = data != null && data.Any() ? data : null;
            _timedLog = new OperationLog(_logger, message, data);
            _logger.Received().Log(LogLevel.Trace, Constants.AtLog, new { Message = message, Data = logData, Operation = "Started", Duration = "" });
        }

        [Theory]
        [InlineData("Some Message", new [] { "with", "data"}, false, "Failed")]
        [InlineData("Some Message", new [] { "with", "data"}, true, "Completed")]
        public void Dispose_Tests(string message, string[] data, bool success, string operation)
        {
            var logData = data != null && data.Any() ? data : null;
            _timedLog = new OperationLog(_logger, message, data);
            if (success)
            {
                _timedLog.Complete();
            }
            
            _timedLog.Dispose();
            
//            _logger.Received().Log(LogLevel.Debug, Constants.AtLog, new { Message = message, Data = logData, Operation = operation , Duration = GetElapsed() });
        }

        private decimal GetElapsed()
        {
            var fieldInfo = _timedLog.GetType().GetField("_stopwatch", BindingFlags.NonPublic | BindingFlags.Instance);
            if (fieldInfo == null) return 0;
            var ticks = (fieldInfo.GetValue(_timedLog) as Stopwatch)?.ElapsedTicks ?? 0;
            return decimal.Round(decimal.Divide(ticks, TimeSpan.TicksPerMillisecond * 100), 2);
        }
    }
}