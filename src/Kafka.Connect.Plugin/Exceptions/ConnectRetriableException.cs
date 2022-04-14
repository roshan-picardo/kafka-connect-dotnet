using System;
using Confluent.Kafka;

namespace Kafka.Connect.Plugin.Exceptions
{
    public class ConnectRetriableException : ConnectException
    {
        public ConnectRetriableException(ErrorCode code, Exception innerException) : base(code, innerException)
        {
        }
    }
}