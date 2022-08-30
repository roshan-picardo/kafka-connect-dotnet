using Kafka.Connect.FunctionalTests.Targets;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.FunctionalTests
{
    public record Config(string Topic, string Schema, string Folder, string[] Files);
    public record Record(JToken Key, JToken Value);
    public record Sink(TargetType Type, string Destination, JToken Setup, JToken Expected, JToken Cleanup);
    public record TestData(string Title, Record[] Records, Sink Sink);

    public record TestCase(string Title, string Topic, dynamic Schema, Record[] Messages, Sink Expected)
    {
        public override string ToString()
        {
            return Title;
        }
    }
}