using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Avro;
using Avro.Generic;
using Kafka.Connect.Utilities;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Converters
{
    public class GenericRecordParser : IGenericRecordParser
    {
        public JToken Parse(GenericRecord genericRecord)
        {
            return ParseRecord(genericRecord);
        }

        private JToken ParseRecord(GenericRecord genericRecord)
        {
            var jsonRecord = new JObject();
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

        private JToken ParseArray(ArraySchema arraySchema, IEnumerable array)
        {
            if (array == null) return null;
            var jsonArray = new JArray();
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

        private JToken ParseUnion(UnionSchema unionSchema, object value)
        {
            if (IsPrimitive(value))
            {
                return ParsePrimitive(GetPrimitiveSchema(unionSchema, value), value);
            }

            return value switch
            {
                GenericRecord genericRecord when unionSchema.Schemas.Any(s =>
                    s.Tag == Schema.Type.Record && s.Name == genericRecord.Schema.Name) =>
                ParseRecord(genericRecord),
                GenericEnum genericEnum when unionSchema.Schemas.Any(s =>
                    s.Tag == Schema.Type.Enumeration && s.Name == genericEnum.Schema.Name) =>
                ParseEnum(genericEnum),
                GenericFixed genericFixed when unionSchema.Schemas.Any(s =>
                    s.Tag == Schema.Type.Fixed && s.Name == genericFixed.Schema.Name) => ParseFixed(
                    genericFixed),
                IList array when
                unionSchema.Schemas.SingleOrDefault(s => s.Tag == Schema.Type.Array) is ArraySchema arraySchema =>
                ParseArray(arraySchema, array),
                IDictionary dictionary when
                unionSchema.Schemas.SingleOrDefault(s => s.Tag == Schema.Type.Map) is MapSchema mapSchema => ParseMap(
                    mapSchema, dictionary as IDictionary<string, object>),
                _ => null
            };
        }

        private JToken ParseMap(MapSchema mapSchema, IDictionary<string, object> dictionary)
        {
            if (dictionary == null) return null;
            var jsonObject = new JObject();
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

        private static JToken ParseEnum(GenericEnum genericEnum)
        {
            return new JValue(genericEnum.Value);
        }

        private static JToken ParsePrimitive(Schema primitiveSchema, object value)
        {
            return value switch
            {
                string s when primitiveSchema.Tag == Schema.Type.String => new JValue(s),
                int i when primitiveSchema.Tag == Schema.Type.Int => new JValue(i),
                bool b when primitiveSchema.Tag == Schema.Type.Boolean => new JValue(b),
                double d when primitiveSchema.Tag == Schema.Type.Double => new JValue(d),
                float f when primitiveSchema.Tag == Schema.Type.Float => new JValue(f),
                long l when primitiveSchema.Tag == Schema.Type.Long => new JValue(l),
                byte[] b when primitiveSchema.Tag == Schema.Type.Bytes => new JValue(b),
                _ when primitiveSchema.Tag == Schema.Type.Null => null,
                _ => throw new SchemaParseException("Unexpected primitive type detected")
            };
        }

        private static JToken ParseFixed(GenericFixed genericFixed)
        {
            return new JValue(genericFixed.Value);
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
                string _ => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.String) as PrimitiveSchema,
                int _ => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Int) as PrimitiveSchema,
                bool _ => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Boolean) as PrimitiveSchema,
                double _ => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Double) as PrimitiveSchema,
                float _ => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Float) as PrimitiveSchema,
                long _ => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Long) as PrimitiveSchema,
                byte[] _ => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Bytes) as PrimitiveSchema,
                _ when value == null => unionSchema.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Null) as
                    PrimitiveSchema,
                _ => throw new SchemaParseException()
            };
        }
    }
}