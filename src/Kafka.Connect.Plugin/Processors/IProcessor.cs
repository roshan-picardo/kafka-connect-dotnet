using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kafka.Connect.Plugin.Processors
{
    public interface IProcessor
    {
        Task<(bool, IDictionary<string, object>)> Apply(IDictionary<string, object> flattened, string connector);

        bool IsOfType(string type);
    }
}

