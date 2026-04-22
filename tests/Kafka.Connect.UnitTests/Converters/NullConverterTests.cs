using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Logging;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Converters;

public class NullConverterTests
{
    private readonly NullConverter _converter = new(Substitute.For<ILogger<NullConverter>>());

    [Fact]
    public async Task Serialize_ReturnsNullBytes()
    {
        var result = await _converter.Serialize("topic", JsonNode.Parse("{}"));

        Assert.Null(result);
    }

    [Fact]
    public async Task Deserialize_ReturnsNullNode()
    {
        var result = await _converter.Deserialize("topic", ReadOnlyMemory<byte>.Empty, new Dictionary<string, byte[]>());

        Assert.Null(result);
    }
}
