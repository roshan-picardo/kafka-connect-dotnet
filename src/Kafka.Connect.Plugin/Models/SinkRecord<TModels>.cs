
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Models
{
    public class SinkRecord<TModel>
    {
        private readonly SinkRecord _sinkRecord;

        public SinkRecord(SinkRecord sinkRecord)
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

        public bool Ready
        {
            get
            {
                if (_sinkRecord.Skip) return false;
                return Models != null && Models.Any();
            }
        }

        public TData GetKey<TData>() => _sinkRecord.GetKey<TData>();
        public TData GetValue<TData>() => _sinkRecord.GetValue<TData>();
        public TData GetMessage<TData>() => _sinkRecord.GetMessage<TData>();

        public SinkStatus Status
        {
            set => _sinkRecord.Status = value;
            get => _sinkRecord.Status;
        }
        public IEnumerable<TModel> Models { get; set; }

        public object LogModels()
        {
            return Models.Select(m => new
            {
                Status,
                Model = JToken.Parse(JsonConvert.SerializeObject(m))
            });
        }
    }
}