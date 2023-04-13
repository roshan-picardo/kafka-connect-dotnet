using System;

namespace Kafka.Connect.Plugin.Exceptions
{
    public class ConnectDataException : ConnectException
    {
        public ConnectDataException(string reason, Exception innerException) : base(reason, innerException)
        {
        }
    }
}