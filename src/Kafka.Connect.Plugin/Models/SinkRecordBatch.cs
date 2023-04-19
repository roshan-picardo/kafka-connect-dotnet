using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Extensions;

namespace Kafka.Connect.Plugin.Models
{
    public class SinkRecordBatch : BlockingCollection<SinkRecord>
    {
        private readonly IList<(string Topic, int Partition, long Offset)> _eofPartitions;
        public SinkRecordBatch(string connector, SinkRecord record = null)
        {
            Connector = connector;
            if (record != null)
            {
                Add(record);
            }
            _eofPartitions = new List<(string Topic, int Partition, long Offset)>();
        }

        public static async Task<SinkRecordBatch> New(string connector)
        {
            return await Task.FromResult(new SinkRecordBatch(connector));
        }
        
        public string Connector { get; }
        
        public bool IsEmpty => Count == 0;

        public SinkRecordBatch Single(SinkRecord record)
        {
            return new SinkRecordBatch(Connector, record);
        }

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
        
        public IEnumerable<(string Topic, int Partition, IEnumerable<T> Batch)> GetByTopicPartition<T>() where T : class
        {
            return from record in this
                group record by new { record.Topic, record.Partition }
                into tp
                select (tp.Key.Topic, tp.Key.Partition, tp.Select(r => r as T));
        }

        public void SetPartitionEof(string topic, int partition, long offset) => _eofPartitions.Add((topic, partition, offset));

        public IEnumerable<(string Topic, int Partition, long Offset)> GetEofPartitions() => _eofPartitions;

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