using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface ISinkExceptionHandler
    {
        void Handle(Exception exception, Action cancelToken);

        Task HandleDeadLetter(ConnectRecordBatch batch, Exception exception, string connector);

        void LogRetryException(ConnectException connectException, int attempts);
    }
}