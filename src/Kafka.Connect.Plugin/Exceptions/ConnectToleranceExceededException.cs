using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Exceptions
{
    public class ConnectToleranceExceededException : ConnectException
    {
        private readonly IEnumerable<SinkRecord> _sinkRecordBatch;
        private readonly ReadOnlyCollection<Exception> _innerExceptions;

        public ConnectToleranceExceededException(ErrorCode code, IEnumerable<SinkRecord> errorBatch, params Exception[] innerExceptions) : base(code)
        {
            _sinkRecordBatch = errorBatch;
            _innerExceptions = new ReadOnlyCollection<Exception>(innerExceptions);
        }
        
        public ConnectToleranceExceededException(ErrorCode code, params Exception[] innerExceptions) : base(code)
        {
            _sinkRecordBatch = null;
            _innerExceptions = new ReadOnlyCollection<Exception>(innerExceptions);
        }

        public bool HasFailedRecords => _sinkRecordBatch != null && _sinkRecordBatch.Any();
        
        public IEnumerable<SinkRecord> GetFailedRecords() =>
            _sinkRecordBatch?
                .Where(r => r.Status == SinkStatus.Failed)
                .OrderBy(r=>r.Topic)
                .ThenBy(r=>r.Partition)
                .ThenBy(r=>r.Offset)
                .Select(r => r);
        
        public override string ToString()
        {
            return ToString(_innerExceptions);
        }
        
        public IEnumerable<Exception> GetAllExceptions() => _innerExceptions;

        public IEnumerable<ConnectException> GetConnectExceptions() =>
            new ReadOnlyCollection<ConnectException>(_innerExceptions.OfType<ConnectException>().ToArray());

        public IEnumerable<Exception> GetNonConnectExceptions() =>
            new ReadOnlyCollection<Exception>(_innerExceptions.Except(GetConnectExceptions()).ToArray());
    }
}