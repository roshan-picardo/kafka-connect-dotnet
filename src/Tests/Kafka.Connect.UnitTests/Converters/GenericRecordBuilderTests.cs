using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;
using Kafka.Connect.Converters;
using Kafka.Connect.Converters.Generic;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Converters;

public class GenericRecordBuilderTests
{
    private readonly GenericRecordBuilder _genericRecordBuilder;

    public GenericRecordBuilderTests()
    {
        _genericRecordBuilder = new GenericRecordBuilder(Substitute.For<ILogger<GenericRecordBuilder>>());
    }

    [Theory]
    [InlineData(Schema.Type.Array)]
    [InlineData(Schema.Type.Boolean)]
    [InlineData(Schema.Type.Bytes)]
    [InlineData(Schema.Type.Double)]
    [InlineData(Schema.Type.Enumeration)]
    [InlineData(Schema.Type.Fixed)]
    [InlineData(Schema.Type.Float)]
    [InlineData(Schema.Type.Int)]
    [InlineData(Schema.Type.Logical)]
    [InlineData(Schema.Type.Long)]
    [InlineData(Schema.Type.Map)]
    [InlineData(Schema.Type.String)]
    [InlineData(Schema.Type.Null)]
    [InlineData(Schema.Type.Union)]
    public void Build_ConnectException_WhenSchemaIsNotRecord(Schema.Type type)
    {
        var json ="{\"type\":\""+ type.ToString().ToLower() +"\",\"name\":\"unit\",\"items\":\"string\",\"size\":3,\"values\":\"string\"}";
        var schema = Schema.Parse(json);
        Assert.Throws<ConnectDataException>(() => _genericRecordBuilder.Build(schema, new JsonObject()));
    }

    [Theory]
    [InlineData(Schema.Type.Boolean, true)]
    [InlineData(Schema.Type.Bytes, new byte[] { 20, 10, 15 })]
    [InlineData(Schema.Type.Double, 38.19)]
    [InlineData(Schema.Type.Float, 38.19F)]
    [InlineData(Schema.Type.Int, 100)]
    [InlineData(Schema.Type.Long, 3800L)]
    [InlineData(Schema.Type.Null, null)]
    [InlineData(Schema.Type.String, "my-simple-id")]
    public void Build_Record_Primitive(Schema.Type type, dynamic value)
    {
        var schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"correlationId\",\"type\":\""+ type.ToString().ToLower() +"\"}]}";
        var recordSchema = (RecordSchema)Schema.Parse(schema);

        var actual = _genericRecordBuilder.Build(recordSchema, new JsonObject { { "correlationId", JsonValue.Create((object)value) } });

        var expected = new GenericRecord(recordSchema);
        expected.Add("correlationId", value?.ToString());
        Assert.Equal(expected.Schema, actual.Schema);
        if (value is not string)
        {
            Assert.Equal(expected.ToString(), actual.ToString());
        }
    }
    
    [Fact]
    public void Build_Record_Enum()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"enumTypeForCorrelationId\",\"type\":\"enum\",\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]}}]}";
        
        var recordSchema = (RecordSchema)Schema.Parse(schema);

        var actual = _genericRecordBuilder.Build(recordSchema, new JsonObject { { "correlationId", "SPADES" } });

        var expected = new GenericRecord(recordSchema);
        expected.Add("correlationId", new GenericEnum((EnumSchema) recordSchema.Fields[0].Schema, "SPADES"));
        Assert.Equal(expected, actual);
    }
    
    [Fact]
    public void Build_Record_Enum_Exception()
    {
        var schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"enumTypeForCorrelationId\",\"type\":\"enum\",\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]}}]}";
        
        var recordSchema = (RecordSchema)Schema.Parse(schema);

        Assert.Throws<ArgumentException>(() => _genericRecordBuilder.Build(recordSchema, new JsonObject { { "correlationId", "NOT_IN_LIST" } }));
    }

    [Theory]
    [InlineData(Schema.Type.Boolean, true, false)]
    [InlineData(Schema.Type.Bytes, new byte[] { 20, 10, 15 }, new byte[] { 200, 110, 115 })]
    [InlineData(Schema.Type.Double, 38.19, 88.19, 38.00)]
    [InlineData(Schema.Type.Float, 38.19F, 99.19F)]
    [InlineData(Schema.Type.Int, 100, 102)]
    [InlineData(Schema.Type.Long, 3800L, 4800L)]
    [InlineData(Schema.Type.Null, null, null)]
    [InlineData(Schema.Type.String, "my-simple-id", "my-simple-second", "my-simple-third")]
    public void Build_Record_Array_Primitive(Schema.Type type, params dynamic[] values)
    {
        var schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"information\",\"type\":{\"name\":\"arrayOfInformation\",\"type\":\"array\",\"items\":\""+ type.ToString().ToLower() +"\"}}]}";
        
        var recordSchema = (RecordSchema)Schema.Parse(schema);

        var actual = _genericRecordBuilder.Build(recordSchema,
            new JsonObject
                { { "information", new JsonArray(values.Select(v =>  (JsonNode)JsonValue.Create((object)v)).ToArray()) } });

        var expected = new GenericRecord(recordSchema);
        expected.Add("information", values);
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Build_Record_Array_Record()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"information\",\"type\":{\"name\":\"arrayOfInformation\",\"type\":\"array\",\"items\":{\"name\":\"employee\",\"type\":\"record\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}}}]}";
        
        var recordSchema = (RecordSchema)Schema.Parse(schema);
        var token = new JsonObject
        {
            {
                "information",
                new JsonArray { new JsonObject { { "id", 1111 }, { "name", "Test" } } }
            }
        };

        var actual = _genericRecordBuilder.Build(recordSchema, token);

        var childRecord = new GenericRecord((recordSchema.Fields[0].Schema as ArraySchema)?.ItemSchema as RecordSchema);
        childRecord.Add("id", 1111);
        childRecord.Add("name", "Test");
        var expected = new GenericRecord(recordSchema);
        expected.Add("information", new[] { childRecord });
        Assert.Equal(expected, actual);
    }
    
    [Fact]
    public void Build_Record_Array_Union()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"information\",\"type\":{\"name\":\"arrayOfInformation\",\"type\":\"array\",\"items\":[\"null\",\"string\",\"double\"]}}]}";
        var recordSchema = (RecordSchema)Schema.Parse(schema);
        var token = JsonNode.Parse("{\"information\": [ null, { \"double\": 10.5}, {\"string\":\"text\" } ]}");

        var actual = _genericRecordBuilder.Build(recordSchema, token);

        var expected = new GenericRecord(recordSchema);
        expected.Add("information", new object[] { null, 10.5, "text" });
        Assert.Equal(expected, actual);
    }
    
    [Fact]
    public void Build_Record_Array_Array()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"information\",\"type\":{\"name\":\"arrayOfInformation\",\"type\":\"array\",\"items\":{\"type\":\"array\",\"name\":\"children\",\"items\":\"string\"}}}]}";
        var recordSchema = (RecordSchema)Schema.Parse(schema);
        var token =  JsonNode.Parse("{\"information\":[[\"item\"]]}");

        var actual = _genericRecordBuilder.Build(recordSchema, token);

        var expected = new GenericRecord(recordSchema);
        expected.Add("information", new object[] {  "item"  });
        Assert.Equal(expected, actual);
    }
    
    [Fact]
    public void Build_Record_Array_Enum()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"information\",\"type\":{\"name\":\"arrayOfInformation\",\"type\":\"array\",\"items\":{\"name\":\"cards\",\"type\":\"enum\",\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]}}}]}";
        var recordSchema = (RecordSchema)Schema.Parse(schema);
        var token = JsonNode.Parse("{\"information\":[\"SPADES\",\"DIAMONDS\",\"CLUBS\"]}");

        var actual = _genericRecordBuilder.Build(recordSchema, token);

        var expected = new GenericRecord(recordSchema);
        var enumSchema = (EnumSchema) ((ArraySchema) recordSchema.Fields[0].Schema).ItemSchema;
        expected.Add("information",
            new[]
            {
                new GenericEnum(enumSchema, "SPADES"),
                new GenericEnum(enumSchema, "DIAMONDS"),
                new GenericEnum(enumSchema, "CLUBS")
            });
        Assert.Equal(expected, actual);
    }
    
    [Fact]
    public void Build_Record_Array_Map()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"information\",\"type\":{\"name\":\"arrayOfInformation\",\"type\":\"array\",\"items\":{\"type\":\"map\",\"name\":\"children\",\"values\":\"string\"}}}]}";
        var recordSchema = (RecordSchema)Schema.Parse(schema);
        var token = JsonNode.Parse("{\"information\":[{\"key1\":\"item\",\"key2\":\"item\"}]}");

        var actual = _genericRecordBuilder.Build(recordSchema, token);

        var expected = new GenericRecord(recordSchema);
        expected.Add("information", new[] {new Dictionary<string, object> {{"key1", "item"}, {"key2", "item"}}});
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Build_Record_Array_Logical()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"information\",\"type\":{\"name\":\"arrayOfInformation\",\"type\":\"array\",\"items\":{\"name\":\"expiry\",\"type\":\"int\",\"logicalType\":\"date\"}}}]}";
        
        var recordSchema = (RecordSchema)Schema.Parse(schema);

        Assert.Throws<NotImplementedException>(() =>
            _genericRecordBuilder.Build(recordSchema, new JsonObject { { "information", new JsonArray(1111, 2222) } }));
    }
    
    [Fact]
    public void Build_Record_Array_Fixed()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"information\",\"type\":{\"name\":\"arrayOfInformation\",\"type\":\"array\",\"items\":{\"name\":\"expiry\",\"type\":\"fixed\",\"size\":32}}}]}";
        
        var recordSchema = (RecordSchema)Schema.Parse(schema);

        Assert.Throws<NotImplementedException>(() =>
            _genericRecordBuilder.Build(recordSchema,
                new JsonObject
                {
                    { "information", new JsonArray { JsonValue.Create("NjFkMjFiZWY5ZTdiNGE5ODgzNmM0MWIwMmVlNDFlMWQ=") } }
                }));

    }

    [Theory]
    [InlineData(Schema.Type.Boolean, true, false)]
    [InlineData(Schema.Type.Bytes, new byte[] { 20, 10, 15 }, new byte[] { 200, 110, 115 })]
    [InlineData(Schema.Type.Double, 38.19, 88.19, 38.00)]
    [InlineData(Schema.Type.Float, 38.19F, 99.19F)]
    [InlineData(Schema.Type.Int, 100, 102)]
    [InlineData(Schema.Type.Long, 385172538158317835L, 485172538158317835L)]
    [InlineData(Schema.Type.Null, null, null)]
    [InlineData(Schema.Type.String, "my-simple-id", "my-simple-second", "my-simple-third")]
    public void Build_Record_Map_Primitive(Schema.Type type, params dynamic[] values)
    {
        var schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"information\",\"type\":{\"name\":\"mapOfInformation\",\"type\":\"map\",\"values\":\""+ type.ToString().ToLower() +"\"}}]}";
        
        var recordSchema = (RecordSchema)Schema.Parse(schema);
        var jObject = new JsonObject();
        var dObject = new Dictionary<string, object>();
        foreach (var value in values)
        {
            dObject.Add($"Key{dObject.Count}", (object)value);
            jObject.Add($"Key{jObject.Count}", JsonValue.Create((object)value));
        }
        
        var actual = _genericRecordBuilder.Build(recordSchema, new JsonObject { { "information", jObject } });

        var expected = new GenericRecord(recordSchema);
        expected.Add("information", dObject);
        Assert.Equal(expected.ToString(), actual.ToString());
    }
    
    [Fact]
    public void Build_Record_Map_Record()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"information\",\"type\":{\"name\":\"arrayOfInformation\",\"type\":\"map\",\"values\":{\"name\":\"employee\",\"type\":\"record\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}}}]}";
        var recordSchema = (RecordSchema)Schema.Parse(schema);
        var token = JsonNode.Parse("{\"information\":{\"Key1\":{\"id\":1111,\"name\":\"name1\"},\"Key2\":{\"id\":2222,\"name\":\"name2\"}}}");

        var actual = _genericRecordBuilder.Build(recordSchema, token);

        var childRecord1 = new GenericRecord((recordSchema.Fields[0].Schema as MapSchema)?.ValueSchema as RecordSchema);
        childRecord1.Add("id", 1111);
        childRecord1.Add("name", "name1");
        var childRecord2 = new GenericRecord((recordSchema.Fields[0].Schema as MapSchema)?.ValueSchema as RecordSchema);
        childRecord2.Add("id", 2222);
        childRecord2.Add("name", "name2");
        var expected = new GenericRecord(recordSchema);
        expected.Add("information",
            new Dictionary<string, object> { { "Key1", childRecord1 }, { "Key2", childRecord2 } });
        Assert.Equal(expected, actual);
    }
    
    [Fact]
    public void Build_Record_Map_Union()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"information\",\"type\":{\"name\":\"mapOfInformation\",\"type\":\"map\",\"values\":[\"null\",\"string\"]}}]}";
        var recordSchema = (RecordSchema)Schema.Parse(schema);
        const string token = "{\"information\": { \"item1\": null, \"item2\":  \"two\" }}";

        var actual = _genericRecordBuilder.Build(recordSchema,  JsonNode.Parse(token));

        var expected = new GenericRecord(recordSchema);
        expected.Add("information", new Dictionary<string, object> { { "item1", null }, { "item2", "two" } });
        Assert.Equal(expected, actual);
    }
}