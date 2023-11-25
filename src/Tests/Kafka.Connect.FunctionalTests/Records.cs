using System.Text.Json.Nodes;
using Kafka.Connect.FunctionalTests.Targets;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.FunctionalTests
{
    public record Config(string Topic, string Schema, string Folder, string[] Files);
    public record Record(JsonNode Key, JsonNode Value);
   // public record Sink(TargetType Type, string Destination, JsonNode Setup, JsonNode Expected, JsonNode Cleanup);
    public record TestData(string Title, Record[] Records);

    public record TestCase(string Title, string Topic, Record Schema, Record[] Messages)
    {
        public override string ToString()
        {
            return Title;
        }
    }
}