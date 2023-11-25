
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Connect.Plugin.Models;

public class ConnectRecord<TModel>
{
    private readonly ConnectRecord _connectRecord;

    public ConnectRecord(ConnectRecord connectRecord)
    {
        _connectRecord = connectRecord;
    }

    public ConnectRecord GetRecord() => _connectRecord;

    public string Topic => _connectRecord.Topic;
    public int Partition => _connectRecord.Partition;
    public long Offset => _connectRecord.Offset;

    public void UpdateStatus()
    {
        _connectRecord.CanCommitOffset = _connectRecord.Skip || Ready;
        _connectRecord.UpdateStatus();
    }

    public bool Ready
    {
        get
        {
            if (_connectRecord.Skip) return false;
            return Models != null && Models.Any();
        }
    }

    public SinkStatus Status
    {
        set => _connectRecord.Status = value;
        get => _connectRecord.Status;
    }
    public IEnumerable<TModel> Models { get; set; }
}