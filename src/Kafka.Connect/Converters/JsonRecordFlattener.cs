using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Converters;

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
            return record.ToJsonNode().ToDictionary();
            //return FlattenInternal(record as JContainer);
        }
    }

    public JToken Unflatten(IDictionary<string, object> dictionary)
    {
        using (_logger.Track("Unflattening the record."))
        {
            return dictionary.ToJson().ToJToken();
        }
    }

    public T ToObject<T>(IDictionary<string, object> dictionary)
    {
        using (_logger.Track("Unflatten to object."))
        {
            return dictionary.ToJson().Deserialize<T>();
        }
    }

    public IDictionary<string, object> Flatten<T>(T data)
    {
        using (_logger.Track("Flatten from object."))
        {
            return JsonSerializer.SerializeToNode(data).ToDictionary();
            //return FlattenInternal(JToken.FromObject(data) as JContainer);
        }
    }
}