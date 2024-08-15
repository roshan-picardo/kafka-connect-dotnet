using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Connect.Plugin.Models;

public class StrategyModel<T>
{
    public Status Status { get; set; }
    public string Topic { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
    
    public Guid Key { get; set; }

    public IList<T> Models { get; set; }

    public T Model
    {
        get
        {
            if (Models?.Any() ?? false)
            {
                return Models[0];
            }

            return default;
        }
        set
        {
            Models ??= new List<T>();
            Models.Add(value);
        }
    }
}