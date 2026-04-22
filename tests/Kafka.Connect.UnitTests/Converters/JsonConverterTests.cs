using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Logging;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Converters;

public class JsonConverterTests
{
    private readonly JsonConverter _converter = new(Substitute.For<ILogger<JsonConverter>>());

    [Fact]
    public async Task Serialize_ReturnsJsonUtf8Bytes()
    {
        var input = JsonNode.Parse("{\"id\":42,\"name\":\"alice\"}");

        var result = await _converter.Serialize("topic", input);

        Assert.Equal("{\"id\":42,\"name\":\"alice\"}", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Deserialize_WhenEmpty_ReturnsNull()
    {
        var result = await _converter.Deserialize("topic", ReadOnlyMemory<byte>.Empty, new Dictionary<string, byte[]>());

        Assert.Null(result);
    }

    [Fact]
    public async Task Deserialize_WhenValidJson_ReturnsJsonNode()
    {
        var bytes = Encoding.UTF8.GetBytes("{\"id\":42}");

        var result = await _converter.Deserialize("topic", bytes, new Dictionary<string, byte[]>());

        Assert.Equal(42, result?["id"]?.GetValue<int>());
    }
}
