
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Connect.Plugin.Models;

public class ConnectRecordModel
{
    protected readonly ConnectRecord ConnectRecord;

    public ConnectRecordModel(ConnectRecord connectRecord)
    {
        ConnectRecord = connectRecord;
    }
}

public class ConnectRecord<TModel> : ConnectRecordModel
{

    public ConnectRecord(ConnectRecord connectRecord) : base(connectRecord)
    {
    }

    public ConnectRecord GetRecord() => ConnectRecord;

    public string Topic => ConnectRecord.Topic;
    public int Partition => ConnectRecord.Partition;
    public long Offset => ConnectRecord.Offset;

    public void UpdateStatus()
    {
        ConnectRecord.CanCommitOffset = ConnectRecord.Skip || Ready;
        ConnectRecord.UpdateStatus();
    }

    public bool Ready
    {
        get
        {
            if (ConnectRecord.Skip) return false;
            return Models != null && Models.Any();
        }
    }

    public SinkStatus Status
    {
        set => ConnectRecord.Status = value;
        get => ConnectRecord.Status;
    }
    public IEnumerable<TModel> Models { get; set; }
}