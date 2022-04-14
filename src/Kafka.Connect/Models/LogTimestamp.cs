using System;
using Newtonsoft.Json;

namespace Kafka.Connect.Models
{
    public class LogTimestamp 
    {
        public long Created { get; set; }
        public long Consumed { get; set; }
        public long Committed { get; set; }
        public int BatchSize { get; set; }
        [JsonIgnore]
        public TimeSpan Lag => TimeSpan.FromMilliseconds(Consumed - Created);
        [JsonIgnore]
        public TimeSpan Total => TimeSpan.FromMilliseconds(Committed - Created);
        [JsonIgnore] 
        public decimal Duration => decimal.Round(decimal.Divide(Committed - Consumed, BatchSize), 2);
        [JsonIgnore]
        public long Batch => Committed - Consumed;
    }
}