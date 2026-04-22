using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Kafka.Connect.Converters;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Converters;

public class AvroConverterTests
{
    private readonly IAsyncSerializer<GenericRecord> _serializer = Substitute.For<IAsyncSerializer<GenericRecord>>();
    private readonly IGenericRecordHandler _genericRecordHandler = Substitute.For<IGenericRecordHandler>();
    private readonly IAsyncDeserializer<GenericRecord> _deserializer = Substitute.For<IAsyncDeserializer<GenericRecord>>();
    private readonly ISchemaRegistryClient _schemaRegistryClient = Substitute.For<ISchemaRegistryClient>();
    private readonly AvroConverter _converter;

    public AvroConverterTests()
    {
        _converter = new AvroConverter(
            Substitute.For<ILogger<AvroConverter>>(),
            _serializer,
            _genericRecordHandler,
            _deserializer,
            _schemaRegistryClient);
    }

    [Fact]
    public async Task Deserialize_UsesDeserializerAndParsesRecord()
    {
        var schema = (RecordSchema)Avro.Schema.Parse("{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}");
        var record = new GenericRecord(schema);
        record.Add("id", 1);
        var headers = new Dictionary<string, byte[]> { ["h"] = [1] };
        var expected = JsonNode.Parse("{\"id\":1}");

        _deserializer.DeserializeAsync(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<bool>(), Arg.Any<SerializationContext>())
            .Returns(record);
        _genericRecordHandler.Parse(record).Returns(expected);

        var result = await _converter.Deserialize("topic-a", new byte[] { 1, 2, 3, 4, 5 }, headers, isValue: false);

        Assert.Equal(1, result?["id"]?.GetValue<int>());
        await _deserializer.Received(1).DeserializeAsync(
            Arg.Any<ReadOnlyMemory<byte>>(),
            false,
            Arg.Is<SerializationContext>(c => c.Component == MessageComponentType.Key && c.Topic == "topic-a"));
        _genericRecordHandler.Received(1).Parse(record);
    }

    [Fact]
    public async Task Serialize_WhenSchemaRegistryThrows_WrapsWithConnectDataException()
    {
        _schemaRegistryClient
            .GetLatestSchemaAsync(Arg.Any<string>())
            .Returns<Task<RegisteredSchema>>(_ => throw new Exception("schema error"));

        var ex = await Assert.ThrowsAsync<ConnectDataException>(() =>
            _converter.Serialize("topic-a", JsonNode.Parse("{}"), subject: "subject-a"));

        Assert.NotNull(ex.InnerException);
        await _serializer.Received(0).SerializeAsync(Arg.Any<GenericRecord>(), Arg.Any<SerializationContext>());
    }
}
