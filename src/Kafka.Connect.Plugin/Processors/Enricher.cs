using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Models;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Processors
{
    public abstract class Enricher : IEnricher
    {
        public abstract Task<IDictionary<string, Message<JToken, JToken>>> Apply(SinkRecord sinkRecord,
            IDictionary<string, string> options);
        
        public bool IsOfType(string type)
        {
            return GetType().FullName == type;
        }
    }
}