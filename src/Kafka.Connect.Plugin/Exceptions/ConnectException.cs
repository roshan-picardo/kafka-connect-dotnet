using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Exceptions
{
    public class ConnectException : Exception
    {
        public ConnectException() : base(ErrorCode.Unknown.GetReason())
        {
        }

        protected ConnectException(ErrorCode code) : base(code.GetReason())
        {
        }

        protected ConnectException(ErrorCode code, Exception innerException) : base(code.GetReason(), innerException)
        {
        }

        private TopicPartitionOffset _topicPartitionOffset;

        public string Topic => _topicPartitionOffset?.Topic ?? "";
        public int Partition => _topicPartitionOffset?.Partition.Value ?? 0;
        public long Offset => _topicPartitionOffset?.Offset.Value ?? 0;

        public Exception SetLogContext(TopicPartitionOffset topicPartitionOffset)
        {
            _topicPartitionOffset = topicPartitionOffset;
            return this;
        }

        public ConnectAggregateException SetLogContext(IEnumerable<SinkRecord> records)
        {
            return new ConnectAggregateException(ErrorCode.Unknown, this is ConnectRetriableException,
                records.Select(SetLogContextAndClone).ToArray());
        }

        private Exception SetLogContextAndClone(SinkRecord record)
        {
            record.Status = SinkStatus.Failed;
            _topicPartitionOffset = record.TopicPartitionOffset;
            return MemberwiseClone() as Exception;
        }
        public Exception SetLogContext(SinkRecord record)
        {
            record.Status = SinkStatus.Failed;
            _topicPartitionOffset = record.TopicPartitionOffset;
            return this;
        }

        public Exception SetLogContext(string topic, int partition, long offset)
        {
            _topicPartitionOffset = new TopicPartitionOffset(topic, new Partition(partition), new Offset(offset));
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
                text.AppendFormat(CultureInfo.InvariantCulture, "(Inner Exception #{0})", i);
                text.Append(innerExceptions[i]);
                text.Append("<---");
                text.AppendLine();
            }

            return text.ToString();
        }
    }
}