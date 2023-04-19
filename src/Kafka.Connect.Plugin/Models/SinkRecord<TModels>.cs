
namespace Kafka.Connect.Plugin.Models
{
    public abstract class SinkRecord<TModels>
    {
        private readonly SinkRecord _sinkRecord;

        protected SinkRecord(SinkRecord sinkRecord)
        {
            _sinkRecord = sinkRecord;
        }

        public SinkRecord GetRecord() => _sinkRecord;

        public string Topic => _sinkRecord.Topic;
        public int Partition => _sinkRecord.Partition;
        public long Offset => _sinkRecord.Offset;

        public void UpdateStatus()
        {
            _sinkRecord.CanCommitOffset = _sinkRecord.Skip || Ready;
            _sinkRecord.UpdateStatus();
        }

        public abstract bool Ready { get; }

        public TData GetKey<TData>() => _sinkRecord.GetKey<TData>();
        public TData GetValue<TData>() => _sinkRecord.GetValue<TData>();
        public TData GetMessage<TData>() => _sinkRecord.GetMessage<TData>();

        public SinkStatus Status
        {
            set => _sinkRecord.Status = value;
            get => _sinkRecord.Status;
        }
        public TModels Models { get; set; }

        public abstract object LogModels();
    }
}