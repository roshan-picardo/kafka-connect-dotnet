using System;

namespace Kafka.Connect.Plugin.Logging
{
    public class LogAttribute : Attribute
    {
        protected LogAttribute(string message, string[] data)
        {
            Message = message;
            Data = data;
        }

        public string Message { get; }
        public string[] Data { get;  }
    }
}