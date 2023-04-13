using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Converters
{
    public class GenericRecordBuilder : IGenericRecordBuilder
    {
        private readonly ILogger<GenericRecordBuilder> _logger;

        public GenericRecordBuilder(ILogger<GenericRecordBuilder> logger)
        {
            _logger = logger;
        }
        public GenericRecord Build(Schema schema, JToken data)
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
        
        private GenericRecord BuildRecord(RecordSchema schema, JToken data)
        {
            var genericRecord = new GenericRecord(schema);
            foreach (var field in schema.Fields)
            {
                switch (field.Schema)
                {
                    case ArraySchema arraySchema:
                        genericRecord.Add(field.Name, BuildArray(arraySchema, data[field.Name] as JArray));
                        break;
                    case UnionSchema unionSchema:
                        genericRecord.Add(field.Name, BuildUnion(unionSchema, data[field.Name]));
                        break;
                    case MapSchema mapSchema:
                        genericRecord.Add(field.Name, BuildMap(mapSchema, data[field.Name] as JObject));
                        break;
                    case LogicalSchema:
                        break;
                    case PrimitiveSchema primitiveSchema:
                        genericRecord.Add(field.Name, BuildPrimitive(primitiveSchema, data[field.Name]));
                        break;
                    case EnumSchema enumSchema:
                        genericRecord.Add(field.Name, BuildEnumeration(enumSchema, data[field.Name]));
                        break;
                    case FixedSchema fixedSchema:
                        genericRecord.Add(field.Name, BuildFixed(fixedSchema));
                        break;
                    case RecordSchema recordSchema:
                        genericRecord.Add(field.Name, BuildRecord(recordSchema, data[field.Name]));
                        break;
                }
            }

            return genericRecord;
        }
        
        private object BuildPrimitive(Schema primitiveSchema, JToken data)
        {
            return primitiveSchema.Tag switch
            {
                Schema.Type.Boolean => data.Value<bool>(),
                Schema.Type.Bytes => data.Value<byte[]>(),
                Schema.Type.Double => data.Value<double>(),
                Schema.Type.Float => data.Value<float>(),
                Schema.Type.Int => data.Value<int>(),
                Schema.Type.Long => data.Value<long>(),
                Schema.Type.Null => null,
                Schema.Type.String =>  data.Value<string>(),
                _ => throw new SchemaParseException("Unexpected primitive type detected.")
            };
        }

        private object BuildEnumeration(EnumSchema enumSchema, JToken data)
        {
            if (!enumSchema.Symbols.Contains(data.Value<string>()))
            {
                throw new ArgumentException("Enum value is not defined.");
            }
            return new GenericEnum(enumSchema, data.Value<string>()); 
        }

        private  object BuildMap(MapSchema mapSchema, JToken data)
        {
            var maps = new Dictionary<string, object>();
            foreach (var (key, value) in data.ToObject<Dictionary<string, object>>())
            {
                switch (mapSchema.ValueSchema)
                {
                    case PrimitiveSchema primitiveSchema:
                        maps.Add(key, BuildPrimitive(primitiveSchema, new JValue(value)));
                        break;
                    case RecordSchema recordSchema:
                        maps.Add(key, BuildRecord(recordSchema, value as JObject));
                        break;
                    case UnionSchema unionSchema:
                        maps.Add(key, BuildUnion(unionSchema, value as JToken));
                        break;
                    case ArraySchema arraySchema:
                        maps.Add(key, BuildArray(arraySchema, value as JArray));
                        break;
                    case EnumSchema enumSchema:
                        maps.Add(key, BuildEnumeration(enumSchema, value as JToken));
                        break;
                    case MapSchema childMapSchema:
                        maps.Add(key, BuildMap(childMapSchema, null));
                        break;
                    case LogicalSchema _:
                        return null;
                    case FixedSchema fixedSchema:
                        maps.Add(key, BuildFixed(fixedSchema));
                        break;
                }
            }

            return maps;
        }

        private object BuildUnion(UnionSchema unionSchema, JToken data)
        {
            Schema childSchema;
            if (data == null || data.Type == JTokenType.Null)
            {
                childSchema = unionSchema.Schemas.SingleOrDefault(s => s.Tag == Schema.Type.Null);
            }
            else if(unionSchema.Schemas.Count(s => s.Tag != Schema.Type.Null) == 1)
            {
                childSchema = unionSchema.Schemas.SingleOrDefault(s => s.Tag != Schema.Type.Null);
            }
            else
            {
                // get the schema by schema name
                childSchema = unionSchema.Schemas.SingleOrDefault(s => s.Name == data["__schema"].Value<string>());
            }
            
            return childSchema switch
            {
                PrimitiveSchema primitiveSchema => BuildPrimitive(primitiveSchema, data),
                RecordSchema recordSchema => BuildRecord(recordSchema, data),
                MapSchema mapSchema => BuildMap(mapSchema, data),
                UnionSchema childUnionSchema => BuildUnion(childUnionSchema, data),
                ArraySchema arraySchema => BuildArray(arraySchema, data as JArray),
                EnumSchema enumSchema => BuildEnumeration(enumSchema, data),
                LogicalSchema _ => BuildLogical(),
                FixedSchema fixedSchema => BuildFixed(fixedSchema),
                _ => null
            };
        }

        private object[] BuildArray(ArraySchema arraySchema, JArray data)
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
                        items.AddRange(BuildArray(childArraySchema, d as JArray));
                        return null;
                    case EnumSchema enumSchema:
                        items.Add(BuildEnumeration(enumSchema, d));
                        break;
                    case MapSchema mapSchema:
                        items.Add(BuildMap(mapSchema, d as JObject));
                        break;
                    case LogicalSchema _:
                        return null;
                    case FixedSchema fixedSchema:
                        items.Add(BuildFixed(fixedSchema));
                        break;
                }
            }

            return items.ToArray();
        }

        private object BuildLogical()
        {
            return null;
        }

        private object BuildFixed(FixedSchema fixedSchema)
        {
            var text = Guid.NewGuid().ToString("N");
            return new GenericFixed(fixedSchema, Encoding.UTF8.GetBytes(text));
        }
    }
}