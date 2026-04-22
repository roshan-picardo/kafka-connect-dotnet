using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Logging;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Converters;

public class StringConverterTests
{
    private readonly StringConverter _converter = new(Substitute.For<ILogger<StringConverter>>());

    [Fact]
    public async Task Serialize_ReturnsUtf8BytesOfJson()
    {
        var result = await _converter.Serialize("topic", JsonNode.Parse("{\"name\":\"alice\"}"));

        Assert.Equal("{\"name\":\"alice\"}", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Deserialize_WhenEmpty_ReturnsNull()
    {
        var result = await _converter.Deserialize("topic", ReadOnlyMemory<byte>.Empty, new Dictionary<string, byte[]>());

        Assert.Null(result);
    }

    [Fact]
    public async Task Deserialize_WhenLengthLessThanFive_ThrowsInvalidDataException()
    {
        await Assert.ThrowsAsync<InvalidDataException>(() =>
            _converter.Deserialize("topic", Encoding.UTF8.GetBytes("abc"), new Dictionary<string, byte[]>()));
    }

    [Fact]
    public async Task Deserialize_WhenValid_ReturnsStringNode()
    {
        var result = await _converter.Deserialize("topic", Encoding.UTF8.GetBytes("hello"), new Dictionary<string, byte[]>());

        Assert.Equal("hello", result?.GetValue<string>());
    }
}
