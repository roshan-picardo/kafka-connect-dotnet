using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kafka.Connect.Plugin.Models
{
    public class SinkRecordBatch : BlockingCollection<SinkRecord>
    {
        private readonly IList<TopicPartitionOffset> _eofPartitions;
        public SinkRecordBatch(string connector, SinkRecord record = null)
        {
            Connector = connector;
            if (record != null)
            {
                Add(record);
            }
            _eofPartitions = new List<TopicPartitionOffset>();
        }

        public static async Task<SinkRecordBatch> New(string connector)
        {
            return await Task.FromResult(new SinkRecordBatch(connector));
        }
        
        public string Connector { get; }
        
        public bool IsEmpty => Count == 0;
        
        public IEnumerable<SinkRecordsByTopicPartition> BatchByTopicPartition =>
            this.GroupBy(g => new {g.Topic, g.Partition})
            .Select(g => new SinkRecordsByTopicPartition
            {
                Topic = g.Key.Topic,
                Partition = g.Key.Partition,
                Batch = g.Select(b => b).ToList()
            }).ToList();

        public void Add(ConsumeResult<byte[], byte[]> consumed)
        {
            Add(new SinkRecord(consumed));
        }

        public SinkRecordBatch Single(SinkRecord record)
        {
            return new SinkRecordBatch(Connector, record);
        }

        public IEnumerable<TopicPartitionOffset> GetCommitReadyOffsets()
        {
            return from record in this
                where record.CanCommitOffset
                select record.TopicPartitionOffset;
        }

        public void MarkAllCommitReady(bool isTolerated = false)
        {
            foreach (var record in this)
            {
                record.CanCommitOffset = record.Status switch
                {
                    SinkStatus.Inserted => true,
                    SinkStatus.Deleted => true,
                    SinkStatus.Updated => true,
                    SinkStatus.Skipped => true,
                    SinkStatus.Excluded => true,
                    SinkStatus.Published => true,
                    SinkStatus.Failed => isTolerated,
                    _ => false
                };
            }
        }

        public void SkipAll()
        {
            foreach (var record in this)
            {
                record.Status = SinkStatus.Skipped;
                record.Skip = true;
            }
        }
        
        public void ExcludeAll()
        {
            foreach (var record in this)
            {
                record.Status = SinkStatus.Excluded;
                record.Skip = true;
            }
        }

        public IEnumerable<SinkRecord> GetAll()
        {
            return this;
        }
        public void SetPartitionEof(TopicPartitionOffset offset)
        {
            _eofPartitions.Add(offset);
        }

        public IEnumerable<TopicPartitionOffset> GetEofPartitions()
        {
            return _eofPartitions;
        }

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

        public void Started()
        {
            foreach (var record in this)
            {
                record.IsOperationCompleted = false;
            }
        }

        public void Completed()
        {
            foreach (var record in this)
            {
                record.IsOperationCompleted = true;
            }
        }
    }
}