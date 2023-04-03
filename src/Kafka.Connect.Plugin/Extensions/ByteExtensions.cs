using System.Text;
using Newtonsoft.Json;

namespace Kafka.Connect.Plugin.Extensions
{
    public static class ByteConvert
    {
        public static byte[] Serialize<T>(T obj)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj));
        }
        
        public static T Deserialize<T>(byte[] data)
        {
            return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(data));
        }
    }
}