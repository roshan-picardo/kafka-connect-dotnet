using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Converters
{
    public interface IRecordFlattener
    {
        IDictionary<string, object>  Flatten(JToken record);
        JToken Unflatten(IDictionary<string, object> dictionary);
        T ToObject<T>(IDictionary<string, object> dictionary);
        IDictionary<string, object> Flatten<T>(T data);
    }
}