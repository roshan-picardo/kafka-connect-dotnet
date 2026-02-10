using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.DynamoDb.Collections;

public interface IDynamoDbQueryRunner
{
    Task WriteMany(IList<WriteRequest> requests, string connector, int taskId, string tableName);
    
    Task<IList<Dictionary<string, AttributeValue>>> ScanMany(
        StrategyModel<ScanModel> model,
        string connector,
        int taskId,
        string tableName);
    
    Task<IList<Dictionary<string, AttributeValue>>> QueryMany(
        StrategyModel<QueryModel> model,
        string connector,
        int taskId,
        string tableName);
}
