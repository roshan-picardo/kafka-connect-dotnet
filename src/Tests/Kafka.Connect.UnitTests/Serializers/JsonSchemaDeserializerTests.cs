using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Serializers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Serializers;

public class JsonSchemaDeserializerTests
{
    private readonly IAsyncDeserializer<JsonNode> _deserializer;
    private readonly JsonSchemaDeserializer _jsonSchemaDeserializer;
        
    public JsonSchemaDeserializerTests()
    {
        _deserializer = Substitute.For<IAsyncDeserializer<JsonNode>>();
        _jsonSchemaDeserializer = new JsonSchemaDeserializer(Substitute.For<ILogger<JsonSchemaDeserializer>>(), _deserializer);
    }
        
    //[Fact]
    public async  Task Deserialize()
    { 
        var expected = new JsonObject {{"value", new JsonObject {{"json", "this is a test sample!"}}}}; 
        _deserializer.DeserializeAsync(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<bool>(),
            Arg.Any<SerializationContext>()).Returns(new JsonObject {{"json", "this is a test sample!"}});

        var actual = await _jsonSchemaDeserializer.Deserialize(ReadOnlyMemory<byte>.Empty, "", null);
            
        Assert.Equal(expected, actual);
    }
}