using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Strategies;

public interface IStrategy
{
    Task<StrategyModel<T>> Build<T>(string connector, IConnectRecord record);
}

public abstract class Strategy<T> : IStrategy
{
    private readonly Regex _regex = new("{(.*?)}", RegexOptions.Compiled);
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
                Key = record.Key,
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
                    Status = Status.Skipping,
                    Topic = record.Topic,
                    Partition = record.Partition,
                    Offset = record.Offset,
                };
        }
    }

    protected string BuildCondition(string condition, IDictionary<string, object> flattened) =>
        _regex.Replace(condition, match => (string)flattened[match.Groups[1].Value]);

    protected abstract Task<StrategyModel<T>> BuildModels(string connector, ConnectRecord record);
    protected abstract Task<StrategyModel<T>> BuildModels(string connector, CommandRecord record);
}