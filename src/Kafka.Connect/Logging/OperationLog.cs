using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Logging
{
    public class OperationLog : IDisposable
    {
        private readonly ILogger _logger;
        private readonly IList<(string key, object value)> _data;
        private readonly Stopwatch _stopwatch;
        private bool _success;

        public OperationLog(ILogger logger, string message, string[] data)
        {
            _logger = logger;
            _stopwatch = Stopwatch.StartNew();
            _data = new List<(string key, object value)>() {("Message", message)};
            if (data != null && data.Any())
            {
                _data.Add(("Data", data));
            }

            var log = GetLogData();
            _logger.LogTrace("{@Log}", new
            {
                Message = log.TryGetValue("Message", out var msg) ? msg : null,
                Data = log.TryGetValue("Data", out var d) ? d : null,
                Operation = log.TryGetValue("Operation", out var operation) ? operation : null,
                Duration = log.TryGetValue("Duration", out var duration) ? duration : null
            });
        }

        public void Complete() => _success = true;

        public void Dispose()
        {
            _stopwatch.Stop();
            var log = GetLogData();
            _logger.LogDebug("{@Log}", new
            {
                Message = log.TryGetValue("Message", out var message) ? message : null,
                Data = log.TryGetValue("Data", out var data) ? data : null,
                Operation = log.TryGetValue("Operation", out var operation) ? operation : null,
                Duration = log.TryGetValue("Duration", out var duration) ? duration : null
            });
        }

        private IDictionary<string, object> GetLogData()
        {
            var dataList = new List<(string key, object value)>(_data).ToDictionary(k => k.key, v => v.value);
            dataList.Add("Operation", _stopwatch.IsRunning ? "Started" : _success ? "Completed" : "Failed");
            if (!_stopwatch.IsRunning)
            {
                dataList.Add("Duration", decimal.Round(decimal.Divide(_stopwatch.ElapsedTicks, TimeSpan.TicksPerMillisecond * 100), 3));
            }

            return dataList;
        }
    }
}