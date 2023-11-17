using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Serializers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Serializers;

public class IgnoreDeserializerTests
{
    [Fact]
    public async  Task Deserialize()
    {
        var ignoreDeserializer = new IgnoreDeserializer(Substitute.For<ILogger<IgnoreDeserializer>>());
            
        var actual = await ignoreDeserializer.Deserialize(ReadOnlyMemory<byte>.Empty, "", new Dictionary<string, byte[]>());
            
        Assert.Null(actual);

    }
}