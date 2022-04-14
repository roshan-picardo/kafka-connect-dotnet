using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Plugin.Logging
{
    public class TimedLog : IDisposable
    {
        private readonly ILogger _logger;
        private readonly IList<(string key, object value)> _data;
        private readonly Stopwatch _stopwatch;
        private bool _success;
        public TimedLog(ILogger logger, IList<(string key, object value)> data)
        {
            _logger = logger;
            _data = data;
            _stopwatch = Stopwatch.StartNew();
            var dataList = new List<(string key, object value)>(data).ToDictionary(k => k.key, v => v.value);
            dataList.Add("Operation", "Started");
            _logger.LogDebug("{@Timing}", dataList);
        }

        public void Complete() => _success = true;

        public void Dispose()
        {
            _stopwatch.Stop();
            var dataList = new List<(string key, object value)>(_data).ToDictionary(k => k.key, v => v.value);
            dataList.Add("Operation", _success ? "Completed" : "Failed");
            dataList.Add("Duration", decimal.Round(decimal.Divide(_stopwatch.ElapsedTicks, TimeSpan.TicksPerMillisecond * 100), 2));
            _logger.LogDebug("{@Timing}", dataList);
        }
    }
}