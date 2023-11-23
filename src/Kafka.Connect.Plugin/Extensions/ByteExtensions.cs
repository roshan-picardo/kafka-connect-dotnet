using System.Text;

namespace Kafka.Connect.Plugin.Extensions
{
    public static class ByteConvert
    {
        public static byte[] Serialize<T>(T obj)
        {
            return Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.SerializeToNode(obj)!.ToJsonString());
        }
    }
}