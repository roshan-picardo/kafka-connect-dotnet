using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Strategies;

public interface IReadWriteStrategy
{
    Task<StrategyModel<T>> Build<T>(string connector, IConnectRecord record);
}

public class StrategyModel<T>
{
    private IList<T> _models;
    
    public SinkStatus Status { get; set; }
    public string Topic { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }

    public IList<T> Models
    {
        get => _models;
        set => _models = value;
    }

    public T Model
    {
        get
        {
            if (_models?.Any() ?? false)
            {
                return _models[0];
            }

            return default;
        }
        set
        {
            _models ??= new List<T>();
            _models.Add(value);
        }
    }
}