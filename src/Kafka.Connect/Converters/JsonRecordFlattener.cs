using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Logging;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Converters
{
    public class JsonRecordFlattener : IRecordFlattener
    {
        private readonly ILogger<JsonRecordFlattener> _logger;

        public JsonRecordFlattener(ILogger<JsonRecordFlattener> logger)
        {
            _logger = logger;
        }
        private readonly Regex _regexFieldNameSeparator =
            new Regex(@"('([^']*)')|(?!\.)([^.^\[\]]+)|(?!\[)(\d+)(?=\])", RegexOptions.Compiled);

        public IDictionary<string, object> Flatten(JToken record)
        {
            using (_logger.Track("Flattening the record."))
            {
                return FlattenInternal(record as JContainer);
            }
        }

        public JToken Unflatten(IDictionary<string, object> dictionary)
        {
            using (_logger.Track("Unflattening the record."))
            {
                return UnflattenInternal(dictionary);
            }
            
        }

        public T ToObject<T>(IDictionary<string, object> dictionary)
        {
            using (_logger.Track("Unflatten to object."))
            {
                return UnflattenInternal(dictionary).ToObject<T>();
            }
        }

        public IDictionary<string, object> Flatten<T>(T data)
        {
            using (_logger.Track("Flatten from object."))
            {
                return FlattenInternal(JToken.FromObject(data) as JContainer);
            }
        }

        private static IDictionary<string, object>
            FlattenInternal(JContainer jsonObject) =>
            jsonObject
                .Descendants()
                .Where(p => !p.Any())
                .Aggregate(new Dictionary<string, object>(), (properties, jToken) =>
                {
                    var value = (jToken as JValue)?.Value;

                    var strVal = jToken.Value<object>()?.ToString()?.Trim();
                    if (strVal?.Equals("[]") == true)
                    {
                        value = Enumerable.Empty<object>();
                    }
                    else if (strVal?.Equals("{}") == true)
                    {
                        value = new object();
                    }

                    properties.Add(jToken.Path, value);

                    return properties;
                });

        private JToken UnflattenInternal(IDictionary<string, object> dictionary)
        {
            var jObject = new JObject();
            foreach (var (key, value) in dictionary)
            {
                var keys = _regexFieldNameSeparator.Matches(key).Select(m => m.Value.Trim('\'')).ToList();
                UnflattenSingle(jObject, keys, value);
            }

            return jObject;
        }

        private static void UnflattenSingle(JToken parent, IList<string> keys, object value)
        {
            while (true)
            {
                JToken next = null;
                switch (parent)
                {
                    case JObject jo:
                    {
                        var current = jo[keys[0]];

                        if (current == null)
                        {
                            if (keys.Count == 1)
                            {
                                switch (value)
                                {
                                    case IEnumerable<object> eValue when !eValue.Any():
                                        current = new JArray();
                                        break;
                                    case null:
                                        current = new JValue((object) null);
                                        break;
                                    default:
                                    {
                                        current = value switch
                                        {
                                            bool _ => new JValue(value),
                                            int _ => new JValue(value),
                                            double _ => new JValue(value),
                                            float _ => new JValue(value),
                                            long _ => new JValue(value),
                                            _ => !value.GetType().GetProperties().Any()
                                                ? new JObject()
                                                : new JValue(value)
                                        };
                                        break;
                                    }
                                }
                            }
                            else if (keys.Count > 1 && int.TryParse(keys[1], out var size))
                            {
                                current = new JArray();
                                for (var i = 0; i <= size; i++)
                                {
                                    if (keys.Count == 2)
                                    {
                                        ((JArray) current).Add(
                                            i == size && value != null ? new JValue(value) : new JValue((object) null));
                                    }
                                    else
                                    {
                                        ((JArray) current).Add(new JObject());
                                    }
                                }

                                keys.RemoveAt(1);
                                next = ((JArray) current)[size];
                            }
                            else
                            {
                                current = new JObject();
                                next = current;
                            }

                            jo.Add(keys[0], current);
                        }
                        else
                        {
                            next = current;
                        }

                        keys.RemoveAt(0);
                        if (keys.Count > 0)
                        {
                            parent = next;
                            continue;
                        }

                        break;
                    }
                    case JArray ja:
                    {
                        if (int.TryParse(keys[0], out var size))
                        {
                            if (ja.Count <= size)
                            {
                                for (var i = ja.Count; i <= size; i++)
                                {
                                    if (keys.Count == 1)
                                    {
                                        ja.Add(new JValue((object) null));
                                    }
                                    else
                                    {
                                        ja.Add(new JObject());
                                    }
                                }
                            }

                            if (keys.Count == 1 && value != null)
                            {
                                ((JValue) ja[size]).Value = value;
                            }

                            next = ja[size];
                        }

                        keys.RemoveAt(0);
                        if (keys.Count > 0)
                        {
                            parent = next;
                            continue;
                        }

                        break;
                    }
                }

                break;
            }
        }
    }
}