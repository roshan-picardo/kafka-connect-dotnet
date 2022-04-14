using System;

namespace Kafka.Connect.Configurations
{
    [Flags]
    public enum RestartsLevel
    {
        None = 0,
        Worker = 1,
        Connector = 2,
        Task = 4,
        All = 7
    }
}