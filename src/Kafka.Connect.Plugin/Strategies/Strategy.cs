using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Strategies;

public abstract class Strategy<T> : IStrategy
{
    private Regex _regex = new("{(.*?)}", RegexOptions.Compiled);
    public async Task<StrategyModel<TType>> Build<TType>(string connector, IConnectRecord record)
    {
        StrategyModel<TType> Convert(StrategyModel<T> response)
        {
            return new StrategyModel<TType>
            {
                Status = response.Status,
                Topic = record.Topic,
                Partition = record.Partition,
                Offset = record.Offset,
                Models = response.Models?.Cast<TType>().ToList()
            };
        }
        
        switch (record)
        {
            case ConnectRecord connectRecord:
            {
                var response = await BuildModels(connector, connectRecord);
                return Convert(response);
            }
            case CommandRecord commandRecord:
            {
                var response = await BuildModels(connector, commandRecord);
                return Convert(response);
            }
            default:
                return new StrategyModel<TType>
                {
                    Status = SinkStatus.Skipping,
                    Topic = record.Topic,
                    Partition = record.Partition,
                    Offset = record.Offset,
                };
        }
    }

    protected string BuildCondition(string condition, IDictionary<string, object> flattened) =>
        _regex.Replace(condition, match => (string)flattened[match.Groups[1].Value]);
    
    protected string BuildCondition(string condition, JsonNode message)
    {
        var regex = new Regex("{(.*?)}", RegexOptions.Compiled);
        var keys = new List<string>();
        foreach (Match match in regex.Matches(condition))
        {
            if (!keys.Contains(match.Value))
            {
                keys.Add(match.Value);
            }
        }

        var parameters = new List<object>();

        foreach (var (key, index) in keys.Select((key, index) => (key, index)))
        {
            condition = condition.Replace($"{{{key}}}", $"{{{index}}}");
            parameters.Add(message[key]);
        }

        return string.Format(condition, parameters.ToArray());
    }

    protected abstract Task<StrategyModel<T>> BuildModels(string connector, ConnectRecord record);
    protected abstract Task<StrategyModel<T>> BuildModels(string connector, CommandRecord record);
}