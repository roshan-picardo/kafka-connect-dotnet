using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Plugin.Logging
{
    public class TimedLogBuilder 
    {
        private readonly ILogger _logger;
        private readonly IList<(string name, object value)> _data;

        public TimedLogBuilder(ILogger logger, string message)
        {
            _logger = logger;
            _data = new List<(string, object)> {("Message", message)};
        }

        public TimedLogBuilder Properties(dynamic data)
        {
            _data.Add(("Details", data));
            return this;
        }

        public void Execute(Action action)
        {
            using var op = new TimedLog(_logger, _data);
            action();
            op.Complete();
        }

        public T Execute<T>(Func<T> function)
        {
            using var op = new TimedLog(_logger, _data);
            var result = function();
            op.Complete();
            return result;
        }
        
        public async Task Execute(Func<Task> action)
        {
            using var op = new TimedLog(_logger, _data);
            await action();
            op.Complete();
        }
        
        public async Task<T> Execute<T>(Func<Task<T>> function)
        {
            using var op = new TimedLog(_logger, _data);
            var result = await function();
            op.Complete();
            return result;
        }
    }
}