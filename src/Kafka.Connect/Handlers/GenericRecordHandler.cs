using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;

namespace Kafka.Connect.Handlers;

public class GenericRecordHandler : IGenericRecordHandler
{
    private readonly ILogger<GenericRecordHandler> _logger;

    public GenericRecordHandler(ILogger<GenericRecordHandler> logger)
    {
        _logger = logger;
    }
    
    public GenericRecord Build(Schema schema, JsonNode data)
    {
        using (_logger.Track("Building generic record."))
        {
            if (schema is RecordSchema recordSchema)
            {
                return BuildRecord(recordSchema, data);
            }

            throw new ConnectDataException(ErrorCode.Local_Fatal.GetReason(),
                new SchemaParseException("Schema of type RecordSchema is expected."));
        }
    }

    public JsonNode Parse(GenericRecord genericRecord)
    {
        using (_logger.Track("Parsing generic record."))
        {
            return ParseRecord(genericRecord);
        }
    }
    
    #region Build
    
    private static object BuildPrimitive(Schema primitiveSchema, JsonNode data)
    {
        return primitiveSchema.Tag switch
        {
            Schema.Type.Boolean => bool.Parse(data?.ToString() ?? string.Empty),
            Schema.Type.Bytes => data.GetValue<byte[]>(),
            Schema.Type.Double =>  double.Parse(data?.ToString() ?? "0"),
            Schema.Type.Float => float.Parse(data?.ToString() ?? "0"),
            Schema.Type.Int => int.Parse(data?.ToString() ?? "0"),
            Schema.Type.Long => long.Parse(data?.ToString() ?? "0"),
            Schema.Type.Null => null,
            Schema.Type.String =>  data?.ToString(),
            _ => throw new SchemaParseException("Unexpected primitive type detected.")
        };
    }

    private object[] BuildArray(ArraySchema arraySchema, JsonArray data)
    {
        if (data == null) return null;
        var items = new List<object>();
        
        foreach (var d in data)
        {
            switch (arraySchema.ItemSchema)
            {
                case PrimitiveSchema primitiveSchema:
                    items.Add(BuildPrimitive(primitiveSchema, d));
                    break;
                case RecordSchema recordSchema:
                    items.Add(BuildRecord(recordSchema, d));
                    break;
                case UnionSchema unionSchema:
                    items.Add(BuildUnion(unionSchema, d));
                    break;
                case ArraySchema childArraySchema:
                    items.AddRange(BuildArray(childArraySchema, d as JsonArray));
                    break;
                case EnumSchema enumSchema:
                    items.Add(BuildEnumeration(enumSchema, d));
                    break;
                case MapSchema mapSchema:
                    items.Add(BuildMap(mapSchema, d));
                    break;
                case LogicalSchema logicalSchema:
                    items.Add(BuildLogical(logicalSchema, d));
                    break;
                case FixedSchema fixedSchema:
                    items.Add(BuildFixed(fixedSchema, d));
                    break;
            }
        }

        return items.ToArray();
    }
    
    private GenericRecord BuildRecord(RecordSchema schema, JsonNode data)
    {
        var genericRecord = new GenericRecord(schema);
        foreach (var field in schema.Fields)
        {
            switch (field.Schema)
            {
                case ArraySchema arraySchema:
                    genericRecord.Add(field.Name, BuildArray(arraySchema, data[field.Name] as JsonArray));
                    break;
                case UnionSchema unionSchema:
                    genericRecord.Add(field.Name, BuildUnion(unionSchema, data[field.Name]));
                    break;
                case MapSchema mapSchema:
                    genericRecord.Add(field.Name, BuildMap(mapSchema, data[field.Name] as JsonObject));
                    break;
                case LogicalSchema logicalSchema:
                    genericRecord.Add(field.Name, BuildLogical(logicalSchema, data));
                    break;
                case PrimitiveSchema primitiveSchema:
                    genericRecord.Add(field.Name, BuildPrimitive(primitiveSchema, data[field.Name]));
                    break;
                case EnumSchema enumSchema:
                    genericRecord.Add(field.Name, BuildEnumeration(enumSchema, data[field.Name]));
                    break;
                case FixedSchema fixedSchema:
                    genericRecord.Add(field.Name, BuildFixed(fixedSchema, data));
                    break;
                case RecordSchema recordSchema:
                    genericRecord.Add(field.Name, BuildRecord(recordSchema, data[field.Name]));
                    break;
            }
        }

        return genericRecord;
    }
    
    private static object BuildEnumeration(EnumSchema enumSchema, JsonNode data)
    {
        if (!enumSchema.Symbols.Contains(data.GetValue<string>()))
        {
            throw new ArgumentException("Enum value is not defined.");
        }
        return new GenericEnum(enumSchema, data.GetValue<string>()); 
    }
    
    private  object BuildMap(MapSchema mapSchema, JsonNode data)
    {
        var maps = new Dictionary<string, object>();
        
        foreach (var (key, value) in data.AsObject())
        {
            switch (mapSchema.ValueSchema)
            {
                case PrimitiveSchema primitiveSchema:
                    maps.Add(key, BuildPrimitive(primitiveSchema,  JsonValue.Create(value?.GetValue<object>())));
                    break;
                case RecordSchema recordSchema:
                    maps.Add(key, BuildRecord(recordSchema, value as JsonObject));
                    break;
                case UnionSchema unionSchema:
                    maps.Add(key,
                        value is null
                            ? BuildUnion(unionSchema, JsonValue.Create(value))
                            : BuildUnion(unionSchema, value));
                    break;
                case ArraySchema arraySchema:
                    maps.Add(key, BuildArray(arraySchema, value as JsonArray));
                    break;
                case EnumSchema enumSchema:
                    maps.Add(key, BuildEnumeration(enumSchema, value));
                    break;
                case MapSchema childMapSchema:
                    maps.Add(key, BuildMap(childMapSchema, null));
                    break;
                case LogicalSchema logicalSchema:
                    maps.Add(key, BuildLogical(logicalSchema, data));
                    break;
                case FixedSchema fixedSchema:
                    maps.Add(key, BuildFixed(fixedSchema, data));
                    break;
            }
        }

        return maps;
    }
    
    private object BuildUnion(UnionSchema unionSchema, JsonNode data)
    {
        Schema childSchema;
        if (data == null)
        {
            childSchema = unionSchema.Schemas.SingleOrDefault(s => s.Tag == Schema.Type.Null);
        }
        else if(unionSchema.Schemas.Count(s => s.Tag != Schema.Type.Null) == 1)
        {
            childSchema = unionSchema.Schemas.SingleOrDefault(s => s.Tag != Schema.Type.Null);
        }
        else if(data is JsonObject jObject)
        {
            if (jObject.Count != 1)
            {
                throw new SchemaParseException("Unable to parse through Union Schema.");
            }
            var property = jObject.Single(); 
            childSchema = unionSchema.Schemas.SingleOrDefault(s => s.Name == property.Key);
            data = property.Value;
        }
        else
        {
            throw new SchemaParseException("Unable to parse through Union Schema.");
        }

        return childSchema switch
        {
            PrimitiveSchema primitiveSchema => BuildPrimitive(primitiveSchema, data),
            RecordSchema recordSchema => BuildRecord(recordSchema, data),
            MapSchema mapSchema => BuildMap(mapSchema, data),
            UnionSchema childUnionSchema => BuildUnion(childUnionSchema, data),
            ArraySchema arraySchema => BuildArray(arraySchema, data as JsonArray),
            EnumSchema enumSchema => BuildEnumeration(enumSchema, data),
            LogicalSchema logicalSchema => BuildLogical(logicalSchema, data),
            FixedSchema fixedSchema => BuildFixed(fixedSchema, data),
            _ => null
        };
    }
    
    private static object BuildLogical(LogicalSchema logicalSchema, JsonNode data)
    {
        throw new NotImplementedException($"Logical Schema Building is not implemented. {logicalSchema}-{data}");
    }

    private static object BuildFixed(FixedSchema fixedSchema, JsonNode data)
    {
        throw new NotImplementedException($"Fixed Schema Building is not implemented. {fixedSchema}-{data}");
    }
    
    #endregion

    #region Parse

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

    #endregion
}