namespace Kafka.Connect.Config.Models
{
    public class TopicConfig
    {
        public string Name { get; init; }
        public SchemaConfig Schema { get; set; }

        public object GetKeySchemaSubjectOrId()
        {
            if (Schema?.Ids != null && Schema.Ids.ContainsKey("key")) return Schema.Ids["key"];
            if (Schema?.Subjects != null && Schema.Subjects.ContainsKey("key")) return Schema.Subjects["key"];
            return $"{Name}-key";
        }
        
        public object GetValueSchemaSubjectOrId()
        {
            if (Schema?.Ids != null && Schema.Ids.ContainsKey("value")) return Schema.Ids["value"];
            if (Schema?.Subjects != null && Schema.Subjects.ContainsKey("value")) return Schema.Subjects["value"];
            return $"{Name}-value";
        }
    }
}