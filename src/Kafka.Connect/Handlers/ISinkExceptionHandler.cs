using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface ISinkExceptionHandler
    {
        void Handle(Exception exception, Action cancelToken);

        Task HandleDeadLetter(IEnumerable<SinkRecord> sinkRecords, Exception exception, string connector);

        void LogRetryException(ConnectException connectException, int attempts);
    }
}