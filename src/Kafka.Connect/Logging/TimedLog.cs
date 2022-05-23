using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Logging
{
    public class TimedLog : IDisposable
    {
        private readonly ILogger _logger;
        private readonly IList<(string key, object value)> _data;
        private readonly Stopwatch _stopwatch;
        private bool _success;

        public TimedLog(ILogger logger, string message, IEnumerable data)
        {
            _logger = logger;
            _stopwatch = Stopwatch.StartNew();
            _data = new List<(string key, object value)>() {("Message", message)};
            if (data != null)
            {
                _data.Add(("Data", data));
            }
            _logger.LogTrace("{@Log}", GetLogData());
        }

        public void Complete() => _success = true;

        public void Dispose()
        {
            _stopwatch.Stop();
            _logger.LogDebug("{@Log}", GetLogData(false));
        }

        private IDictionary<string, object> GetLogData(bool begin = true)
        {
            var dataList = new List<(string key, object value)>(_data).ToDictionary(k => k.key, v => v.value);
            dataList.Add("Operation", begin ? "Started" : _success ? "Completed" : "Failed");
            if (!begin)
            {
                dataList.Add("Duration", decimal.Round(decimal.Divide(_stopwatch.ElapsedTicks, TimeSpan.TicksPerMillisecond * 100), 2));
            }

            return dataList;
        }
    }
}