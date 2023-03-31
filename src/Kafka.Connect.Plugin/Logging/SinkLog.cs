using System;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Plugin.Logging
{
    public class SinkLog : IDisposable
    {
        private readonly ILogger _logger;
        private readonly string _message;
        private readonly Stopwatch _stopwatch;

        public SinkLog(ILogger logger, string message)
        {
            _logger = logger;
            _message = message;
            _stopwatch = Stopwatch.StartNew();

            _logger.LogTrace("{@Log}", new
            {
                Message = _message,
                Operation = "Started"
            });
        }

        public void Dispose()
        {
            _stopwatch.Stop();
            _logger.LogDebug("{@Log}", new
            {
                Message = _message,
                Operation = "Finished",
                Duration = decimal.Round(decimal.Divide(_stopwatch.ElapsedTicks, TimeSpan.TicksPerMillisecond * 100), 3)
            });
        }
    }
}