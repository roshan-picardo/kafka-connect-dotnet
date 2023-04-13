using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Exceptions
{
    public class ConnectException : Exception
    {
        public ConnectException() : base("Unknown")
        {
        }

        protected ConnectException(string message) : base(message)
        {
        }

        protected ConnectException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public string Topic { get; private set; }
        public int Partition { get; private set; }
        public long Offset { get; private set; }

        public ConnectAggregateException SetLogContext(IEnumerable<SinkRecord> records)
        {
            return new ConnectAggregateException("Unknown", this is ConnectRetriableException,
                records.Select(SetLogContextAndClone).ToArray());
        }

        private Exception SetLogContextAndClone(SinkRecord record)
        {
            record.Status = SinkStatus.Failed;
            Topic = record.Topic;
            Partition = record.Partition;
            Offset = record.Offset;
            return MemberwiseClone() as Exception;
        }
        public Exception SetLogContext(SinkRecord record)
        {
            record.Status = SinkStatus.Failed;
            Topic = record.Topic;
            Partition = record.Partition;
            Offset = record.Offset;
            return this;
        }

        protected string ToString(ReadOnlyCollection<Exception> innerExceptions)
        {
            var text = new StringBuilder();
            text.Append(base.ToString());

            for (var i = 0; i < innerExceptions.Count; i++)
            {
                if (innerExceptions[i] == InnerException)
                    continue; 

                text.Append(Environment.NewLine).Append("--->");
                text.Append(CultureInfo.InvariantCulture, $"(Inner Exception #{i})");
                text.Append(innerExceptions[i]);
                text.Append("<---");
                text.AppendLine();
            }

            return text.ToString();
        }
    }
}