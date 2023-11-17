using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Avro.Generic;
using Confluent.Kafka;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Serializers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Serializers;

public class AvroDeserializerTests
{
    private readonly IAsyncDeserializer<GenericRecord> _deserializer;
    private readonly IGenericRecordParser _parser;

    private readonly AvroDeserializer _avroDeserializer;

    public AvroDeserializerTests()
    {
        _deserializer = Substitute.For<IAsyncDeserializer<GenericRecord>>();
        _parser = Substitute.For<IGenericRecordParser>();
            
        _avroDeserializer = new AvroDeserializer(Substitute.For<ILogger<AvroDeserializer>>(), _deserializer, _parser);
            
    }

    [Fact]
    public async Task AvroDeserializer_TestAsIs()
    {
        var expected = JsonValue.Create("this is a test sample!");
        _parser.Parse(Arg.Any<GenericRecord>()).Returns("this is a test sample!");
            
        var actual = await _avroDeserializer.Deserialize(new ReadOnlyMemory<byte>(), "", new Dictionary<string, byte[]>());

        await _deserializer.Received().DeserializeAsync(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<bool>(),
            Arg.Any<SerializationContext>());
        _parser.Received().Parse(Arg.Any<GenericRecord>());
        Assert.Equal(expected.ToJsonString(), actual.ToJsonString());
    }
}