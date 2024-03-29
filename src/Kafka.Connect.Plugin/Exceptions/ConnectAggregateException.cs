using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Kafka.Connect.Plugin.Exceptions
{
    public class ConnectAggregateException : ConnectException
    {
        private readonly ReadOnlyCollection<Exception> _innerExceptions;
        private readonly bool _canRetry;

        public ConnectAggregateException(string reason, Exception innerException, bool canRetry = false) : base(reason)
        {
            _canRetry = canRetry;
            _innerExceptions = new ReadOnlyCollection<Exception>(new [] { innerException } );
        }

        public ConnectAggregateException(string reason, bool canRetry = false, params Exception[] innerExceptions) : base(reason)
        {
            _canRetry = canRetry;
            _innerExceptions = new ReadOnlyCollection<Exception>(innerExceptions);
        }

        public bool ShouldRetry => _innerExceptions.Any() &&  _innerExceptions.All(i => i is ConnectRetriableException); 
        
        // one or inner exceptions can be retried... and this should trigger un-batching
        public bool CanRetry =>  _canRetry || _innerExceptions.Any(i => i is ConnectRetriableException);
        
        public override string ToString()
        {
            return ToString(_innerExceptions);
        }

        public IEnumerable<ConnectException> GetConnectExceptions() =>
            new ReadOnlyCollection<ConnectException>(_innerExceptions.OfType<ConnectException>().ToArray());

        public IEnumerable<Exception> GetNonConnectExceptions() =>
            new ReadOnlyCollection<Exception>(_innerExceptions.Except(GetConnectExceptions()).ToArray());

        public IEnumerable<Exception> GetAllExceptions() => _innerExceptions;
    }
}