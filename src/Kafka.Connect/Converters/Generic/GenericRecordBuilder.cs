using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Schema = Avro.Schema;

namespace Kafka.Connect.Converters.Generic;

public class GenericRecordBuilder : IGenericRecordBuilder
{
    private readonly ILogger<GenericRecordBuilder> _logger;

    public GenericRecordBuilder(ILogger<GenericRecordBuilder> logger)
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
                        value is not JsonNode token
                            ? BuildUnion(unionSchema, JsonValue.Create(value))
                            : BuildUnion(unionSchema, token));
                    break;
                case ArraySchema arraySchema:
                    maps.Add(key, BuildArray(arraySchema, value as JsonArray));
                    break;
                case EnumSchema enumSchema:
                    maps.Add(key, BuildEnumeration(enumSchema, value));
                    break;
                case MapSchema childMapSchema:
                    maps.Add(key, BuildMap(childMapSchema, (JsonObject)null));
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
        throw new NotImplementedException("Logical Schema Building is not implemented.");
    }

    private static object BuildFixed(FixedSchema fixedSchema, JsonNode data)
    {
        throw new NotImplementedException("Fixed Schema Building is not implemented.");
    }
}