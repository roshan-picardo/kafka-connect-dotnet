using System;
using Confluent.Kafka;

namespace Kafka.Connect.Plugin.Exceptions
{
    public class ConnectDataException : ConnectException
    {
        public ConnectDataException(ErrorCode code, Exception innerException) : base(code, innerException)
        {
        }
    }
}