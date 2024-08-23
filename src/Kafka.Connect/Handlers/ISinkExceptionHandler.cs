using System;
using Kafka.Connect.Plugin.Exceptions;

namespace Kafka.Connect.Handlers
{
    public interface ISinkExceptionHandler
    {
        void Handle(Exception exception, Action cancelToken);

        void LogRetryException(ConnectException connectException, int attempts);
    }
}