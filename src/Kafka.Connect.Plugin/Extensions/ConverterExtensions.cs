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
        new Regex(@"('([^']*)')|(?!\.)([^.^\[\]]+)|(?!\[)(\d+)(?=\])", RegexOptions.Compiled);
    
    public static Dictionary<string, object> ToDictionary(this JsonNode token, string prefix = "")
    {
        object GetValue(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.String:
                    return element.GetString();
                case JsonValueKind.Number when element.TryGetInt32(out var intValue):
                    return intValue;
                case JsonValueKind.Number when element.TryGetInt64(out var longValue):
                    return longValue;
                case JsonValueKind.Number when element.TryGetDouble(out var doubleValue):
                    return doubleValue;
                case JsonValueKind.Number:
                    return 0;
                case JsonValueKind.True or JsonValueKind.False:
                    return element.GetBoolean();
                case JsonValueKind.Undefined or JsonValueKind.Null:
                    return null;
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
        
        var result = new Dictionary<string, object>();
        switch (token)
        {
            case JsonValue jv:
                result.Add(GetKey(jv), GetValue(jv.GetValue<JsonElement>()));
                return result;
            case JsonObject jo:
                if (jo.Count == 0 && jo.ToJsonString() == "{}")
                {
                    result.Add(GetKey(jo), new object());
                }
                else
                {
                    foreach (var (_, node) in jo)
                    {
                        foreach (var (key, value) in ToDictionary(node))
                        {
                            result.Add(key, value);
                        }
                    }
                }

                return result;
            case JsonArray ja:
                if (ja.Count == 0 && ja.ToJsonString() == "[]")
                {
                    result.Add(GetKey(ja), Enumerable.Empty<object>());
                }
                else
                {
                    foreach (var node in ja)
                    {
                        foreach (var (key, value) in ToDictionary(node))
                        {
                            result.Add(key, value);
                        }
                    }
                }

                return result;
        }

        return result;
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