using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Extensions;

public static class ConverterExtensions
{
    private static readonly Regex RegexFieldNameSeparator =
        new(@"('([^']*)')|(?!\.)([^.^\[\]]+)|(?!\[)(\d+)(?=\])", RegexOptions.Compiled);

    public static Dictionary<string, object> ToDictionary(this JsonNode node, string prefix = "")
    {
        object GetValue(JsonNode jn)
        {
            switch (jn)
            {
                case JsonObject:
                    return new object();
                case JsonArray:
                    return Array.Empty<object>();
                case JsonValue:
                    var je = jn.GetValue<JsonElement>();
                    switch (je.ValueKind)
                    {
                        case JsonValueKind.String:
                            return je.GetString();
                        case JsonValueKind.Number when je.TryGetInt32(out var intValue):
                            return intValue;
                        case JsonValueKind.Number when je.TryGetInt64(out var longValue):
                            return longValue;
                        case JsonValueKind.Number when je.TryGetSingle(out var singleValue):
                            return singleValue;
                        case JsonValueKind.Number when je.TryGetDouble(out var doubleValue):
                            return doubleValue;
                        case JsonValueKind.Number:
                            return 0;
                        case JsonValueKind.True or JsonValueKind.False:
                            return je.GetBoolean();
                        case JsonValueKind.Undefined or JsonValueKind.Null:
                            return null;
                    }
                    break;
            }

            return null;
        }

        string GetKey(JsonNode jn)
        {
            var key = jn.GetPath().TrimStart('$', '.').Replace("['", "").Replace("']", "");
            if (!string.IsNullOrEmpty(prefix))
            {
                key = $"{prefix}.{key}";
            }

            return key;
        }

        IEnumerable<JsonNode> Parse(JsonNode jn)
        {
            return jn switch
            {
                JsonValue jv => new List<JsonNode> { jv },
                JsonObject jo => ParseObject(jo),
                JsonArray ja => ParseArray(ja),
                _ => null
            };
        }
        
        List<JsonNode> ParseObject(JsonObject jo)
        {
            var nodes = new List<JsonNode>();
            if (jo.Count == 0 && jo.ToJsonString() == "{}")
            {
                nodes.Add(jo); 
            }
            else
            {
                foreach (var (_, value) in jo)
                {
                    nodes.AddRange(Parse(value));
                }
            }

            return nodes;
        }
        
        List<JsonNode> ParseArray(JsonArray ja)
        {
            var nodes = new List<JsonNode>();
            if (ja.Count == 0 && ja.ToJsonString() == "[]")
            {
                nodes.Add(ja);
            }
            else
            {
                foreach (var item in ja)
                {
                    nodes.AddRange(Parse(item));
                }
            }

            return nodes;
        }

        return Parse(node)?.ToDictionary(GetKey, GetValue);
    }

    public static JsonNode ToJson(this IDictionary<string, object> flattened)
    {
        var result = new Dictionary<string, object>();
        string previousKey = null;
        var previousIndex = -1;
        foreach (var (fullKey, value) in flattened)
        {
            var segments = RegexFieldNameSeparator.Matches(fullKey).Select(m => m.Value.Trim('\'')).ToList();
            var loop = result;
            List<object> loopList = null;
            do
            {
                var key = segments.FirstOrDefault();

                if (previousKey != null)
                {
                    loop ??= new Dictionary<string, object>();
                    if (int.TryParse(key, out _))
                    {
                        if (!loop.ContainsKey(previousKey))
                        {
                            loop.Add(previousKey, new List<object>());
                        }

                        loopList = loop[previousKey] as List<object>;
                    }
                    else if (previousIndex >= 0)
                    {
                        loopList ??= new List<object>();
                        if (key == null)
                        {
                            while (loopList.Count <= previousIndex)
                            {
                                loopList.Add(new object());
                            }   
                            loopList[previousIndex] = value;
                        }
                        else
                        {
                            while (loopList.Count <= previousIndex)
                            {
                                loopList.Add(new Dictionary<string, object>());
                            }
                            loop = loopList[previousIndex] as Dictionary<string, object>;
                        }
                    }
                    else if (key == null)
                    {
                        loop.Add(previousKey, value);
                    }
                    else if (!loop.ContainsKey(previousKey))
                    {
                        loop.Add(previousKey, new Dictionary<string, object>());
                    }

                    if (!int.TryParse(key, out var keyIndex))
                    {
                        if (previousIndex == -1)
                        {
                            loop = loop[previousKey] as Dictionary<string, object>;
                        }

                        previousKey = key;
                        previousIndex = -1;
                    }
                    else
                    {
                        previousIndex = keyIndex;
                    }
                }
                else
                {
                    previousKey = key;
                }

                if (segments.Count > 0)
                {
                    segments.RemoveAt(0);
                }
                else
                {
                    segments = null;
                }
            } while (segments != null);
        }
        
        var jsonString = JsonSerializer.Serialize(result, new JsonSerializerOptions
        {
            WriteIndented = true
        });
        return JsonNode.Parse(jsonString);
    }
    
    public static JToken ToJToken(this JsonNode jsonNode) => JToken.Parse(jsonNode.ToJsonString());
    
    public static JsonNode ToJsonNode(this JToken jToken) => JsonNode.Parse(jToken.ToString());
}