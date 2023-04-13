using System;

namespace Kafka.Connect.Plugin.Exceptions
{
    public class ConnectRetriableException : ConnectException
    {
        public ConnectRetriableException(string reason, Exception innerException) : base(reason, innerException)
        {
        }
    }
}