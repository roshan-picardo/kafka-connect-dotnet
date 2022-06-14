using System.Collections.Generic;

namespace Kafka.Connect.Configurations
{
    public class ProcessorConfig
    {
        public string Name { get; set; }
        public int Order { get; set; }
        public IList<string> Topics { get; set; }
    }
}