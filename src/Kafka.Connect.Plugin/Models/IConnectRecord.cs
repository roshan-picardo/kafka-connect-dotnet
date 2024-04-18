using System;

namespace Kafka.Connect.Plugin.Models;

public interface IConnectRecord
{
    Exception Exception { get; set; }
    string Topic { get;  }
    int Partition { get;  }
    long Offset { get;  }
}
