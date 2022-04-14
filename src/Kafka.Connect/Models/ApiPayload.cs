using System.Collections.Generic;

namespace Kafka.Connect.Models
{
    public class ApiPayload
    {
        public int? Delay { get; set; }
        public IDictionary<string, string> Payload { get; set; }
    }
}