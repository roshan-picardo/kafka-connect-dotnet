namespace Kafka.Connect.Plugin
{
    public static class Constants
    {
        public const string Key = "key";
        public const string Value = "value";
        public const string Topic = "Topic";
        public const string Partition = "Partition";
        public const string Offset = "Offset";
        public const string AtLog = "{@Log}";
        public const string DefaultDeserializer = "Kafka.Connect.Serializers.AvroDeserializer";
    }
}