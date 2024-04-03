using System;

namespace Kafka.Connect.Plugin.Models;

public interface IConnectRecord
{
    Exception Exception { get; set; }
}
