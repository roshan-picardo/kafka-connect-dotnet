using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;
using Constants = Kafka.Connect.Plugin.Constants;

namespace Kafka.Connect.Processors
{
    public static class ProcessorHelper 
    {
        private static string ReplaceBrackets(string field)
        {
            return field.Replace("[", "<").Replace("]", ">");
        }
        
        private static string ReplaceStarAndBrackets(string field)
        {
            return ReplaceBrackets(field).Replace("*", "([a-zA-Z0-9.]{0,})");
        }

        public static IEnumerable<string> GetMatchingKeys(this IEnumerable<string> options, IDictionary<string, object> flattened)
        {
            var flattenedKeys = new List<string>();
            foreach (var field in options ?? Enumerable.Empty<string>())
            {
                if (ReplaceBrackets(field).Contains('*'))
                {
                    flattenedKeys.AddRange(flattened.Keys.Where(k =>
                        LikeOperator.LikeString(ReplaceBrackets(k), ReplaceBrackets(field), CompareMethod.Text)));
                }
                else if (flattened.ContainsKey(field))
                {
                    flattenedKeys.Add(field);
                }
            }

            return flattenedKeys.Distinct().ToList();
        }

        public static IDictionary<string, string> GetMatchingMaps(this IDictionary<string, string> maps, IDictionary<string, object> flattened, bool keyOnly = false)
        {
            maps ??= new Dictionary<string, string>();
            var flattenedMaps = new Dictionary<string, string>();
            var sb = new StringBuilder();
            foreach (var (key, value) in maps)
            {
                if (ReplaceBrackets(key).Contains('*'))
                {
                    var keys = flattened.Keys.Where(k =>
                        LikeOperator.LikeString(ReplaceBrackets(k), ReplaceBrackets(key), CompareMethod.Text));
                    //Regex way forward
                    var regex = new Regex(ReplaceStarAndBrackets(key), RegexOptions.Compiled);
                    foreach (var k in keys)
                    {
                        if (keyOnly)
                        {
                            flattenedMaps.Add(k, value);
                            continue;
                        }

                        sb.Clear();
                        var matchIndex = 1;
                        var match = regex.Match(ReplaceBrackets(k));
                        if (value.StartsWith("*"))
                        {
                            sb.Append(match.Groups[matchIndex++]);
                        }

                        foreach (var split in value.Split("*"))
                        {
                            sb.Append(split).Append(match.Groups[matchIndex++]);
                        }

                        if (value.EndsWith("*"))
                        {
                            sb.Append(match.Groups[matchIndex]);
                        }

                        flattenedMaps.Add(k, sb.ToString());
                    }
                }
                else if (flattened.ContainsKey(key))
                {
                    flattenedMaps.Add(key, value);
                }
            }

            return flattenedMaps;
        }

        public static string Prefix(this string key)
        {
            return key.StartsWith($"{Constants.Key}.") || key.StartsWith($"{Constants.Value}.")
                ? key
                : $"{Constants.Value}.{key}";
        }
    }
}