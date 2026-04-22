using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Logging;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Converters;

public class JsonSchemaConverterTests
{
    private readonly IAsyncSerializer<JsonNode> _serializer = Substitute.For<IAsyncSerializer<JsonNode>>();
    private readonly IAsyncDeserializer<JsonNode> _deserializer = Substitute.For<IAsyncDeserializer<JsonNode>>();
    private readonly JsonSchemaConverter _converter;

    public JsonSchemaConverterTests()
    {
        _converter = new JsonSchemaConverter(
            Substitute.For<ILogger<JsonSchemaConverter>>(), _serializer, _deserializer);
    }

    [Fact]
    public async Task Serialize_UsesSerializerAndReturnsBytes()
    {
        var payload = JsonNode.Parse("{\"name\":\"alice\"}");
        var headers = new Dictionary<string, byte[]> { ["h1"] = [1, 2, 3] };
        _serializer.SerializeAsync(Arg.Any<JsonNode>(), Arg.Any<SerializationContext>()).Returns([10, 20]);

        var result = await _converter.Serialize("topic-a", payload, headers: headers, isValue: false);

        Assert.Equal(new byte[] { 10, 20 }, result);
        await _serializer.Received(1).SerializeAsync(
            payload,
            Arg.Is<SerializationContext>(c =>
                c.Component == MessageComponentType.Key &&
                c.Topic == "topic-a" &&
                c.Headers != null));
    }

    [Fact]
    public async Task Deserialize_PassesIsNullFlagAndReturnsNode()
    {
        var headers = new Dictionary<string, byte[]>();
        var expected = JsonNode.Parse("{\"status\":\"ok\"}");
        _deserializer.DeserializeAsync(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<bool>(), Arg.Any<SerializationContext>())
            .Returns(expected);

        var result = await _converter.Deserialize("topic-b", ReadOnlyMemory<byte>.Empty, headers, isValue: true);

        Assert.Equal("ok", result?["status"]?.GetValue<string>());
        await _deserializer.Received(1).DeserializeAsync(
            Arg.Any<ReadOnlyMemory<byte>>(),
            true,
            Arg.Is<SerializationContext>(c => c.Component == MessageComponentType.Value && c.Topic == "topic-b"));
    }
}
