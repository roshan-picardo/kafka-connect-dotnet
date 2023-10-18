using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Extensions;

namespace Kafka.Connect.Plugin.Models
{
    public class ConnectRecordBatch : BlockingCollection<ConnectRecord>
    {
        private readonly IList<(string Topic, int Partition, long Offset)> _eofPartitions;
        public ConnectRecordBatch(string connector, ConnectRecord record = null)
        {
            Connector = connector;
            if (record != null)
            {
                Add(record);
            }
            _eofPartitions = new List<(string Topic, int Partition, long Offset)>();
        }

        public string Connector { get; }
        
        public bool IsEmpty => Count == 0;

        public int EofCount => _eofPartitions.Count;

        public IEnumerable<(string Topic, int Partition, long Offset)> GetCommitReadyOffsets()
        {
            return from record in this
                where record.CanCommitOffset
                select (record.Topic, record.Partition, record.Offset);
        }

        public void MarkAllCommitReady(bool isTolerated = false)
        {
            this.ForEach(record => record.CanCommitOffset = record.Status switch
            {
                SinkStatus.Inserted => true,
                SinkStatus.Deleted => true,
                SinkStatus.Updated => true,
                SinkStatus.Skipped => true,
                SinkStatus.Excluded => true,
                SinkStatus.Published => true,
                SinkStatus.Failed => isTolerated,
                _ => false
            });
        }

        public void SkipAll()
        {
            this.ForEach(record =>
            {
                record.Status = SinkStatus.Skipped;
                record.Skip = true;
            });
        }
        
        public void ExcludeAll()
        {
            this.ForEach(record =>
            {
                record.Status = SinkStatus.Excluded;
                record.Skip = true;
            });
        }

        public IEnumerable<T> GetAll<T>() where T : class => this.Select(r => r as T);

        public bool Any<T>(Func<T, bool> condition) => this.Select(r => r.GetValue<T>()).Any(condition);
        
        public IEnumerable<(string Topic, int Partition, IEnumerable<T> Batch)> GetByTopicPartition<T>() where T : class
        {
            return from record in this
                group record by new { record.Topic, record.Partition }
                into tp
                select (tp.Key.Topic, tp.Key.Partition, tp.Select(r => r as T));
        }

        public void SetPartitionEof(string topic, int partition, long offset) => _eofPartitions.Add((topic, partition, offset));

        public IList<(string Topic, int Partition, long Offset)> GetEofPartitions() => _eofPartitions;

        public dynamic GetBatchStatus()
        {
            return (from record in this
                group record by record.Status
                into grpStatus
                select new
                {
                    Status = grpStatus.Key, 
                    Count = grpStatus.Count()
                }).ToArray();
        }
        
        public bool IsLastAttempt { get; set; }

        public void Started() => this.ForEach(record => record.IsOperationCompleted = false);

        public void Completed() => this.ForEach(record => record.IsOperationCompleted = true);
    }
}