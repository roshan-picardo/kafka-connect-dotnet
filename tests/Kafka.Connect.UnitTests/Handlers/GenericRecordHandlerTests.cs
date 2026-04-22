using System;
using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Handlers;

public class GenericRecordHandlerTests
{
    private readonly GenericRecordHandler _handler = new(Substitute.For<ILogger<GenericRecordHandler>>());

    [Theory]
    [InlineData(Schema.Type.Array)]
    [InlineData(Schema.Type.String)]
    [InlineData(Schema.Type.Int)]
    public void Build_WhenSchemaIsNotRecord_ThrowsConnectDataException(Schema.Type type)
    {
        var schema = Schema.Parse($"{{\"type\":\"{type.ToString().ToLower()}\",\"name\":\"unit\",\"items\":\"string\"}}");

        Assert.Throws<ConnectDataException>(() => _handler.Build(schema, new JsonObject()));
    }

    [Fact]
    public void Build_WhenEnumValueInvalid_ThrowsArgumentException()
    {
        const string schemaJson = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"card\",\"type\":{\"name\":\"cardType\",\"type\":\"enum\",\"symbols\":[\"SPADES\",\"HEARTS\"]}}]}";
        var schema = (RecordSchema)Schema.Parse(schemaJson);

        Assert.Throws<ArgumentException>(() => _handler.Build(schema, new JsonObject { ["card"] = "CLUBS" }));
    }

    [Fact]
    public void Parse_WhenRecordContainsEnum_ReturnsJsonValue()
    {
        const string schemaJson = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"card\",\"type\":{\"name\":\"cardType\",\"type\":\"enum\",\"symbols\":[\"SPADES\",\"HEARTS\"]}}]}";
        var schema = (RecordSchema)Schema.Parse(schemaJson);
        var record = new GenericRecord(schema);
        record.Add("card", new GenericEnum((EnumSchema)schema.Fields[0].Schema, "SPADES"));

        var actual = _handler.Parse(record);

        Assert.Equal("SPADES", actual?["card"]?.GetValue<string>());
    }

    [Fact]
    public void Parse_WhenLogicalSchemaEncountered_ThrowsNotImplementedException()
    {
        const string schemaJson = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"expires\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}]}";
        var schema = (RecordSchema)Schema.Parse(schemaJson);
        var record = new GenericRecord(schema);
        record.Add("expires", 100);

        Assert.Throws<NotImplementedException>(() => _handler.Parse(record));
    }
}
