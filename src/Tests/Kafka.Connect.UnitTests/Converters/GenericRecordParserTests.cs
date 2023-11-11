using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Logging;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Converters;

public class GenericRecordParserTests
{
    private readonly GenericRecordParser _genericRecordParser;

    public GenericRecordParserTests()
    {
        _genericRecordParser = new GenericRecordParser(Substitute.For<ILogger<GenericRecordParser>>());
    }

    [Theory]
    [InlineData("string", "correlationId")]
    [InlineData("int", 100)]
    [InlineData("boolean", true)]
    [InlineData("double", 38.19)]
    [InlineData("float", 15.18F)]
    [InlineData("long", 500L)]
    [InlineData("null", null)]
    public void Parse_Record_Primitive_Types(string tag, dynamic value)
    {
        var schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":\"" + tag + "\"}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId", value);

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", value}};
        Assert.Equivalent(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_EnumSchema()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"enumTypeForCorrelationId\",\"type\":\"enum\",\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var enumSchema = (EnumSchema) recordSchema.Fields[0].Schema;
        record.Add("correlationId", new GenericEnum(enumSchema, "SPADES"));

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", "SPADES"}};
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Theory]
    [InlineData("string", "one", "two")]
    [InlineData("int", 100, 300)]
    [InlineData("boolean", true, false)]
    [InlineData("double", 38.19, 50.11, 60.12)]
    [InlineData("float", 15.18F, 19.66F)]
    [InlineData("long", 500L, 900L, 1000L, 1L)]
    public void Parse_Record_Array_Primitive_Schema(string tag, params dynamic[] values)
    {
        var schema =
            "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"arrayOfCorrelationId\",\"type\":\"array\",\"items\":\""+ tag +"\"}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId", values);

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId",  JsonValue.Create(values)}};
        Assert.Equivalent(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Array_Array_String()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"arrayOfCorrelationId\",\"type\":\"array\",\"items\":{\"type\":\"array\",\"name\":\"childArray\",\"items\":\"string\"}}}]}";
            
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId", new[] {new[] {"item"}});

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", new JsonArray {new JsonArray {"item"}}}};
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Array_Union_String()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"arrayOfCorrelationId\",\"type\":\"array\",\"items\":[\"null\",\"string\"]}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId", new[] {null, "abc", "xyz"});

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", new JsonArray {null, "abc", "xyz"}}};
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Array_Map_String()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"arrayOfCorrelationId\",\"type\":\"array\",\"items\":{\"type\":\"map\",\"name\":\"childMap\",\"values\":\"string\"}}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId", new[] {new Dictionary<string, object> {{"key1", "item"}, {"key2", "item"}}});

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject
        {
            {
                "correlationId",
                new JsonArray {new JsonObject {{"key1", "item"}, {"key2", "item"}}}
            }
        };
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Array_Enum()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"arrayOfCorrelationId\",\"type\":\"array\",\"items\":{\"name\":\"enumTypeForCorrelationId\",\"type\":\"enum\",\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]}}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var enumSchema = (EnumSchema) ((ArraySchema) recordSchema.Fields[0].Schema).ItemSchema;

        //new GenericEnum( enumSchema, "SPADES")
        record.Add("correlationId",
            new[]
            {
                new GenericEnum(enumSchema, "SPADES"),
                new GenericEnum(enumSchema, "DIAMONDS"),
                new GenericEnum(enumSchema, "CLUBS")
            });

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", new JsonArray {"SPADES", "DIAMONDS", "CLUBS"}}};
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Array_Record()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"arrayOfCorrelationId\",\"type\":\"array\",\"items\":{\"name\":\"enumTypeForCorrelationId\",\"type\":\"record\",\"fields\":[{\"name\":\"firstPart\",\"type\":\"int\"},{\"name\":\"secondPart\",\"type\":\"string\"}]}}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var childRecord =
            new GenericRecord((RecordSchema) ((ArraySchema) recordSchema.Fields[0].Schema).ItemSchema);

        childRecord.Add("firstPart", 1111);
        childRecord.Add("secondPart", "123344");


        record.Add("correlationId", new[] {childRecord});

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject
        {
            {
                "correlationId",
                new JsonArray {new JsonObject {{"firstPart", 1111}, {"secondPart", "123344"}}}
            }
        };
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Array_Logical()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"arrayOfCorrelationId\",\"type\":\"array\",\"items\":{\"name\":\"enumTypeForCorrelationId\",\"type\":\"int\",\"logicalType\":\"date\"}}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);

        record.Add("correlationId",
            new[]
            {
                111111,
                222222,
                333333
            });

        Assert.Throws<NotImplementedException>(() => _genericRecordParser.Parse(record));
    }

    [Fact]
    public void Parse_Record_Array_Fixed()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"arrayOfCorrelationId\",\"type\":\"array\",\"items\":{\"name\":\"enumTypeForCorrelationId\",\"type\":\"fixed\",\"size\":32}}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var fixedSchema = (FixedSchema) ((ArraySchema) recordSchema.Fields[0].Schema).ItemSchema;
        var fixedRecord = new GenericFixed(fixedSchema, Encoding.UTF8.GetBytes("61d21bef9e7b4a98836c41b02ee41e1d"));
        record.Add("correlationId",
            new[]
            {
                fixedRecord
            });

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject
        {
            {
                "correlationId",
                new JsonArray {JsonValue.Create("NjFkMjFiZWY5ZTdiNGE5ODgzNmM0MWIwMmVlNDFlMWQ=")}
            }
        };
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Theory]
    [InlineData("string", "one")]
    [InlineData("int", 100)]
    [InlineData("boolean", true)]
    [InlineData("double", 38.19)]
    [InlineData("float", 15.18F)]
    [InlineData("long", 500L)]
    public void Parse_Record_Union_Primitive_Schema(string tag, dynamic value)
    {
        var schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":[\"null\",\""+ tag +"\"]}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId", value);

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", value}};
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Union_Record()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":[{\"name\":\"enumTypeForCorrelationId\",\"type\":\"record\",\"fields\":[{\"name\":\"firstPart\",\"type\":\"int\"},{\"name\":\"secondPart\",\"type\":\"string\"}]}]}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var childRecord =
            new GenericRecord((RecordSchema) ((UnionSchema) recordSchema.Fields[0].Schema).Schemas[0]);

        childRecord.Add("firstPart", 1111);
        childRecord.Add("secondPart", "123344");


        record.Add("correlationId", childRecord);

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject
        {
            {
                "correlationId",
                new JsonObject {{"firstPart", 1111}, {"secondPart", "123344"}}
            }
        };
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Union_Enum()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":[{\"name\":\"enumTypeForCorrelationId\",\"type\":\"enum\",\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]}]}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var enumSchema = (EnumSchema) ((UnionSchema) recordSchema.Fields[0].Schema).Schemas.First();

        record.Add("correlationId", new GenericEnum(enumSchema, "CLUBS"));

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", "CLUBS"}};
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Union_Fixed()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":[{\"name\":\"enumTypeForCorrelationId\",\"type\":\"fixed\",\"size\":32}]}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var fixedSchema = (FixedSchema) ((UnionSchema) recordSchema.Fields[0].Schema).Schemas.First();
        var fixedRecord = new GenericFixed(fixedSchema, Encoding.UTF8.GetBytes("61d21bef9e7b4a98836c41b02ee41e1d"));
        record.Add("correlationId", fixedRecord);

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject()
        {
            {
                "correlationId", "NjFkMjFiZWY5ZTdiNGE5ODgzNmM0MWIwMmVlNDFlMWQ="
            }
        };
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Union_Array()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":[{\"type\":\"array\",\"name\":\"childArray\",\"items\":\"string\"}]}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId", new[] {"item"});

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", new JsonArray {"item"}}};

        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Union_Map()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":[{\"type\":\"map\",\"name\":\"childMap\",\"values\":\"string\"}]}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId", new Dictionary<string, object> {{"key1", "item"}, {"key2", "item"}});

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject
        {
            {
                "correlationId",
                new JsonObject {{"key1", "item"}, {"key2", "item"}}
            }
        };
        Assert.Equal(expected.ToString(), actual.ToString());
    }



    [Theory]
    [InlineData("string", "one")]
    [InlineData("int", 100)]
    [InlineData("boolean", true)]
    [InlineData("double", 38.19)]
    [InlineData("float", 15.18F)]
    [InlineData("long", 500L)]
    public void Parse_Record_Map_Primitive(string tag, dynamic values)
    {
        var schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"arrayOfCorrelationId\",\"type\":\"map\",\"values\":\""+ tag +"\"}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId", new Dictionary<string, object> {{"key1", values}, {"key2", values}});

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", new JsonObject {{"key1", values}, {"key2", values}}}};
        Assert.Equivalent(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Map_Record()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"arrayOfCorrelationId\",\"type\":\"map\",\"values\":{\"name\":\"recordTypeForCorrelationId\",\"type\":\"record\",\"fields\":[{\"name\":\"firstPart\",\"type\":\"int\"},{\"name\":\"secondPart\",\"type\":\"string\"}]}}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var childRecord =
            new GenericRecord((RecordSchema) ((MapSchema) recordSchema.Fields[0].Schema).ValueSchema);
        childRecord.Add("firstPart", 1111);
        childRecord.Add("secondPart", "123344");
        record.Add("correlationId", new Dictionary<string, object> {{"key1", childRecord}});

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject
        {
            {
                "correlationId",
                new JsonObject {{"key1", new JsonObject {{"firstPart", 1111}, {"secondPart", "123344"}}}}
            }
        };
        Assert.Equal(expected.ToString(), actual.ToString());
    }
        
    [Fact]
    public void Parse_Record_Map_Enum()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"mapOfCorrelationId\",\"type\":\"map\",\"values\":{\"name\":\"enumTypeForCorrelationId\",\"type\":\"enum\",\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]}}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var enumSchema = (EnumSchema) ((MapSchema) recordSchema.Fields[0].Schema).ValueSchema;

        record.Add("correlationId",
            new Dictionary<string, object> {{"cards", new GenericEnum(enumSchema, "CLUBS")}});

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", new JsonObject {{"cards", "CLUBS"}}}};
        Assert.Equal(expected.ToString(), actual.ToString());
    }
        
    [Fact]
    public void Parse_Record_Map_Fixed()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"mapOfCorrelationId\",\"type\":\"map\",\"values\":{\"name\":\"fixedTypeForCorrelationId\",\"type\":\"fixed\",\"size\":32}}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var fixedSchema = (FixedSchema) ((MapSchema) recordSchema.Fields[0].Schema).ValueSchema;
        var fixedRecord = new GenericFixed(fixedSchema, Encoding.UTF8.GetBytes("61d21bef9e7b4a98836c41b02ee41e1d"));
        record.Add("correlationId", new Dictionary<string, object> {{"fixed", fixedRecord}});


        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject
        {
            {
                "correlationId", new JsonObject {{ "fixed", "NjFkMjFiZWY5ZTdiNGE5ODgzNmM0MWIwMmVlNDFlMWQ="}}
            }
        };
        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Map_Array()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"mapOfCorrelationId\",\"type\":\"map\",\"values\":{\"name\":\"arrayTypeForCorrelationId\",\"type\":\"array\",\"items\":\"string\"}}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId", new Dictionary<string, object> {{"items", new[] {"item1", "item2"}}});

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", new JsonObject {{"items", new JsonArray {"item1", "item2"}}}}};

        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Map_Map()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"mapOfCorrelationId\",\"type\":\"map\",\"values\":{\"name\":\"arrayTypeForCorrelationId\",\"type\":\"map\",\"values\":\"string\"}}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId",
            new Dictionary<string, object> {{"item1", new Dictionary<string, object> {{"item2", "value"}}}});

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject {{"correlationId", new JsonObject {{"item1", new JsonObject {{"item2", "value"}}}}}};

        Assert.Equal(expected.ToString(), actual.ToString());
    }

    [Fact]
    public void Parse_Record_Map_Union()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"mapOfCorrelationId\",\"type\":\"map\",\"values\":[\"null\",\"string\"]}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId",
            new Dictionary<string, object> {{"item1", null}, {"item2", "two"}});

        var actual = _genericRecordParser.Parse(record); 

        var expected = new JsonObject {{"correlationId", new JsonObject {{"item1", null}, {"item2", "two"}}}};

        Assert.Equal(expected.ToString(), actual.ToString());
    }
        
    [Fact]
    public void Parse_Record_Map_Logical()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"mapOfCorrelationId\",\"type\":\"map\",\"values\":{\"name\":\"logicalCorrelationId\",\"type\":\"int\",\"logicalType\":\"date\"}}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId",
            new Dictionary<string, object> {{"item1", 111111}});

        Assert.Throws<NotImplementedException>(() => _genericRecordParser.Parse(record));
    }

    [Fact]
    public void Parse_Record_Record()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":\"string\"},{\"name\":\"details\",\"type\":{\"type\":\"record\",\"name\":\"details\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var childRecordSchema = (RecordSchema) recordSchema.Fields[1].Schema;
        var childRecord = new GenericRecord(childRecordSchema);
        childRecord.Add("id", 100);
        childRecord.Add("name", "test-unit");
        record.Add("correlationId", "correlationId");
        record.Add("details", childRecord);

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject
        {
            {"correlationId", "correlationId"},
            {"details", new JsonObject {{"id", 100}, {"name", "test-unit"}}}
        };
        Assert.Equal(expected.ToString(), actual.ToString());
    }
        
    [Fact]
    public void Parse_Record_Fixed()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"correlationId\",\"type\":\"fixed\",\"size\":32}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        var fixedSchema = (FixedSchema) recordSchema.Fields[0].Schema;
        var fixedRecord = new GenericFixed(fixedSchema, Encoding.UTF8.GetBytes("61d21bef9e7b4a98836c41b02ee41e1d"));
        record.Add("correlationId", fixedRecord);

        var actual = _genericRecordParser.Parse(record);

        var expected = new JsonObject
        {
            {
                "correlationId", "NjFkMjFiZWY5ZTdiNGE5ODgzNmM0MWIwMmVlNDFlMWQ="
            }
        };
        Assert.Equal(expected.ToString(), actual.ToString());
    }
        
        
    [Fact]
    public void Parse_Record_Logical()
    {
        const string schema = "{\"type\":\"record\",\"name\":\"unit\",\"fields\":[{\"name\":\"correlationId\",\"type\":{\"name\":\"correlationId\",\"type\":\"int\",\"logicalType\":\"date\"}}]}";
        var recordSchema = (RecordSchema) Schema.Parse(schema);
        var record = new GenericRecord(recordSchema);
        record.Add("correlationId", 11111);

        Assert.Throws<NotImplementedException>(() => _genericRecordParser.Parse(record));
    }
}