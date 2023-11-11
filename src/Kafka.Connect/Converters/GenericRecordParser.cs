using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;
using Kafka.Connect.Plugin.Logging;

namespace Kafka.Connect.Converters;

public class GenericRecordParser : IGenericRecordParser
{
    private readonly ILogger<GenericRecordParser> _logger;

    public GenericRecordParser(ILogger<GenericRecordParser> logger)
    {
        _logger = logger;
    }
    public JsonNode Parse(GenericRecord genericRecord)
    {
        using (_logger.Track("Parsing generic record."))
        {
            return ParseRecord(genericRecord);
        }
    }
    
    private JsonNode ParseRecord(GenericRecord genericRecord)
    {
        var jsonRecord = new JsonObject();
        foreach (var field in genericRecord.Schema.Fields)
        {
            if (genericRecord.TryGetValue(field.Name, out var value))
            {
                switch (field.Schema)
                {
                    case ArraySchema arraySchema when value is IList array:
                        jsonRecord.Add(field.Name, ParseArray(arraySchema, array));
                        break;
                    case UnionSchema unionSchema:
                        jsonRecord.Add(field.Name, ParseUnion(unionSchema, value));
                        break;
                    case MapSchema mapSchema when value is IDictionary map:
                        jsonRecord.Add(field.Name, ParseMap(mapSchema, map as IDictionary<string, object>));
                        break;
                    case PrimitiveSchema primitiveSchema:
                        jsonRecord.Add(field.Name, ParsePrimitive(primitiveSchema, value));
                        break;
                    case EnumSchema _ when value is GenericEnum @enum:
                        jsonRecord.Add(field.Name, ParseEnum(@enum));
                        break;
                    case RecordSchema _ when value is GenericRecord record:
                        jsonRecord.Add(field.Name, ParseRecord(record));
                        break;
                    case LogicalSchema _: 
                        throw new NotImplementedException("Logical Schema Parsing is not implemented.");
                    case FixedSchema _ when value is GenericFixed genericFixed:
                        jsonRecord.Add(field.Name, ParseFixed(genericFixed));
                        break;
                    default:
                        throw new SchemaParseException(
                            $"Unexpected schema mapping found. Field: {field.Name}, Schema Type: {field.Schema.Tag}, Value Type: {value.GetType()} ");
                }
            }
        }

        return jsonRecord;
    }

    private JsonNode ParseArray(ArraySchema arraySchema, IEnumerable array)
    {
        if (array == null) return null;
        var jsonArray = new JsonArray();
        foreach (var entry in array)
        {
            switch (arraySchema.ItemSchema)
            {
                case ArraySchema childArraySchema when entry is IList childArray:
                    jsonArray.Add(ParseArray(childArraySchema, childArray));
                    break;
                case UnionSchema unionSchema:
                    jsonArray.Add(ParseUnion(unionSchema, entry));
                    break;
                case MapSchema mapSchema when entry is IDictionary map:
                    jsonArray.Add(ParseMap(mapSchema, map as IDictionary<string, object>));
                    break;
                case PrimitiveSchema primitiveSchema:
                    jsonArray.Add(ParsePrimitive(primitiveSchema, entry));
                    break;
                case EnumSchema _ when entry is GenericEnum @enum:
                    jsonArray.Add(ParseEnum(@enum));
                    break;
                case RecordSchema _ when entry is GenericRecord record:
                    jsonArray.Add(ParseRecord(record));
                    break;
                case LogicalSchema _:
                    throw new NotImplementedException("Logical Schema Parsing is not implemented.");
                case FixedSchema _ when entry is GenericFixed genericFixed:
                    jsonArray.Add(ParseFixed(genericFixed));
                    break;
                default:
                    throw new SchemaParseException(
                        $"Unexpected schema mapping found. Field: field.Name, Schema Type: {arraySchema.ItemSchema.Tag}, Value Type: {entry.GetType()} ");
            }
        }

        return jsonArray;
    }

    private JsonNode ParseUnion(UnionSchema unionSchema, object value)
    {
        if (IsPrimitive(value))
        {
            return ParsePrimitive(GetPrimitiveSchema(unionSchema, value), value);
        }

        return value switch
        {
            GenericRecord genericRecord when unionSchema.Schemas.Any(s =>
                    s.Tag == Schema.Type.Record && s.Name == genericRecord.Schema.Name) => ParseRecord(genericRecord),
            GenericEnum genericEnum when unionSchema.Schemas.Any(s =>
                    s.Tag == Schema.Type.Enumeration && s.Name == genericEnum.Schema.Name) => ParseEnum(genericEnum),
            GenericFixed genericFixed when unionSchema.Schemas.Any(s =>
                s.Tag == Schema.Type.Fixed && s.Name == genericFixed.Schema.Name) => ParseFixed(genericFixed),
            IList array when
                unionSchema.Schemas.SingleOrDefault(s => s.Tag == Schema.Type.Array) is ArraySchema arraySchema => ParseArray(arraySchema, array),
            IDictionary dictionary when
                unionSchema.Schemas.SingleOrDefault(s => s.Tag == Schema.Type.Map) is MapSchema mapSchema => ParseMap(mapSchema, dictionary as IDictionary<string, object>),
            _ => null
        };
    }

    private JsonNode ParseMap(MapSchema mapSchema, IDictionary<string, object> dictionary)
    {
        if (dictionary == null) return null;
        var jsonObject = new JsonObject();
        foreach (var (key, value) in dictionary)
        {
            switch (mapSchema.ValueSchema)
            {
                case ArraySchema arraySchema when value is IList array:
                    jsonObject.Add(key, ParseArray(arraySchema, array));
                    break;
                case UnionSchema unionSchema:
                    jsonObject.Add(key, ParseUnion(unionSchema, value));
                    break;
                case MapSchema childMapSchema when value is IDictionary childMap:
                    jsonObject.Add(key, ParseMap(childMapSchema, childMap as IDictionary<string, object>));
                    break;
                case PrimitiveSchema primitiveSchema:
                    jsonObject.Add(key, ParsePrimitive(primitiveSchema, value));
                    break;
                case EnumSchema _ when value is GenericEnum @enum:
                    jsonObject.Add(key, ParseEnum(@enum));
                    break;
                case RecordSchema _ when value is GenericRecord record:
                    jsonObject.Add(key, ParseRecord(record));
                    break;
                case LogicalSchema _: 
                    throw new NotImplementedException("Logical Schema Parsing is not implemented.");
                case FixedSchema _ when value is GenericFixed genericFixed:
                    jsonObject.Add(key, ParseFixed(genericFixed));
                    break;
                default:
                    throw new SchemaParseException(
                        $"Unexpected schema mapping found. Field: field.Name, Schema Type: {mapSchema.ValueSchema.Tag}, Value Type: {value.GetType()} ");
            }
        }

        return jsonObject;
    }

    private static JsonNode ParseEnum(GenericEnum genericEnum)
    {
        return JsonValue.Create(genericEnum.Value);
    }

    private static JsonNode ParsePrimitive(Schema primitiveSchema, object value)
    {
        return value switch
        {
            string s when primitiveSchema.Tag == Schema.Type.String => JsonValue.Create(s),
            int i when primitiveSchema.Tag == Schema.Type.Int => JsonValue.Create(i),
            bool b when primitiveSchema.Tag == Schema.Type.Boolean => JsonValue.Create(b),
            double d when primitiveSchema.Tag == Schema.Type.Double => JsonValue.Create(d),
            float f when primitiveSchema.Tag == Schema.Type.Float => JsonValue.Create(f),
            long l when primitiveSchema.Tag == Schema.Type.Long => JsonValue.Create(l),
            byte[] b when primitiveSchema.Tag == Schema.Type.Bytes => JsonValue.Create(b),
            _ when primitiveSchema.Tag == Schema.Type.Null => null,
            _ => throw new SchemaParseException("Unexpected primitive type detected")
        };
    }

    private static JsonNode ParseFixed(GenericFixed genericFixed)
    {
        return JsonValue.Create(genericFixed.Value);
    }

    private static bool IsPrimitive(object value)
    {
        return value switch
        {
            string _ => true,
            int _ => true,
            bool _ => true,
            double _ => true,
            float _ => true,
            long _ => true,
            byte[] _ => true,
            _ when value == null => true,
            _ => false
        };
    }

    private static PrimitiveSchema GetPrimitiveSchema(UnionSchema unionSchema, object value)
    {
        return value switch
        {
            string => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.String) as PrimitiveSchema,
            int => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Int) as PrimitiveSchema,
            bool => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Boolean) as PrimitiveSchema,
            double => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Double) as PrimitiveSchema,
            float => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Float) as PrimitiveSchema,
            long => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Long) as PrimitiveSchema,
            byte[] => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Bytes) as PrimitiveSchema,
            null => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Null) as PrimitiveSchema,
            _ => throw new SchemaParseException()
        };
    }
}