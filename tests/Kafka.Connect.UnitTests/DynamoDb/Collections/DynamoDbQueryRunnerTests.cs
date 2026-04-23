using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb.Collections;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.DynamoDb.Collections;

public class DynamoDbQueryRunnerTests
{
    [Fact]
    public async Task WriteMany_WhenRequestsNull_ReturnsWithoutClientCall()
    {
        var logger = Substitute.For<ILogger<DynamoDbQueryRunner>>();
        var clientProvider = Substitute.For<IDynamoDbClientProvider>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("connector").Returns(new PluginConfig());

        var sut = new DynamoDbQueryRunner(logger, clientProvider, configProvider);

        await sut.WriteMany(null, "connector", 1, "table");

        clientProvider.DidNotReceive().GetDynamoDbClient(Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact]
    public async Task WriteMany_WhenRequestsEmpty_ReturnsWithoutClientCall()
    {
        var logger = Substitute.For<ILogger<DynamoDbQueryRunner>>();
        var clientProvider = Substitute.For<IDynamoDbClientProvider>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("connector").Returns(new PluginConfig());

        var sut = new DynamoDbQueryRunner(logger, clientProvider, configProvider);

        await sut.WriteMany(new List<WriteRequest>(), "connector", 1, "table");

        clientProvider.DidNotReceive().GetDynamoDbClient(Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact]
    public async Task WriteMany_WhenMoreThan25Requests_SplitsIntoBatches()
    {
        var logger = Substitute.For<ILogger<DynamoDbQueryRunner>>();
        var clientProvider = Substitute.For<IDynamoDbClientProvider>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("connector").Returns(new PluginConfig());

        var client = Substitute.For<IAmazonDynamoDB>();
        client.BatchWriteItemAsync(Arg.Any<BatchWriteItemRequest>(), Arg.Any<CancellationToken>())
            .Returns(new BatchWriteItemResponse
            {
                UnprocessedItems = new Dictionary<string, List<WriteRequest>>()
            });
        clientProvider.GetDynamoDbClient("connector", 1).Returns(client);

        var requests = new List<WriteRequest>();
        for (var i = 0; i < 30; i++)
        {
            requests.Add(new WriteRequest
            {
                PutRequest = new PutRequest
                {
                    Item = new Dictionary<string, AttributeValue> { ["id"] = new() { N = i.ToString() } }
                }
            });
        }

        var sut = new DynamoDbQueryRunner(logger, clientProvider, configProvider);

        await sut.WriteMany(requests, "connector", 1, "table");

        await client.Received(2).BatchWriteItemAsync(Arg.Any<BatchWriteItemRequest>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task WriteMany_WhenDynamoDbThrows_ThrowsConnectRetriableException()
    {
        var logger = Substitute.For<ILogger<DynamoDbQueryRunner>>();
        var clientProvider = Substitute.For<IDynamoDbClientProvider>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("connector").Returns(new PluginConfig());

        var client = Substitute.For<IAmazonDynamoDB>();
        client.BatchWriteItemAsync(Arg.Any<BatchWriteItemRequest>(), Arg.Any<CancellationToken>())
            .Returns<Task<BatchWriteItemResponse>>(_ => throw new AmazonDynamoDBException("boom"));
        clientProvider.GetDynamoDbClient("connector", 1).Returns(client);

        var requests = new List<WriteRequest>
        {
            new()
            {
                PutRequest = new PutRequest
                {
                    Item = new Dictionary<string, AttributeValue> { ["id"] = new() { N = "1" } }
                }
            }
        };

        var sut = new DynamoDbQueryRunner(logger, clientProvider, configProvider);

        await Assert.ThrowsAsync<ConnectRetriableException>(() => sut.WriteMany(requests, "connector", 1, "table"));
    }

    [Fact]
    public async Task ScanMany_ReturnsItemsFromClient()
    {
        var logger = Substitute.For<ILogger<DynamoDbQueryRunner>>();
        var clientProvider = Substitute.For<IDynamoDbClientProvider>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        var client = Substitute.For<IAmazonDynamoDB>();

        var expected = new List<Dictionary<string, AttributeValue>>
        {
            new() { ["id"] = new AttributeValue { N = "1" } }
        };

        client.ScanAsync(Arg.Any<ScanRequest>(), Arg.Any<CancellationToken>())
            .Returns(new ScanResponse { Items = expected });
        clientProvider.GetDynamoDbClient("connector", 1).Returns(client);

        var model = new StrategyModel<ScanModel>
        {
            Model = new ScanModel { Request = new ScanRequest { TableName = "table" } }
        };

        var sut = new DynamoDbQueryRunner(logger, clientProvider, configProvider);

        var result = await sut.ScanMany(model, "connector", 1, "table");

        Assert.Single(result);
    }

    [Fact]
    public async Task QueryMany_ReturnsItemsFromClient()
    {
        var logger = Substitute.For<ILogger<DynamoDbQueryRunner>>();
        var clientProvider = Substitute.For<IDynamoDbClientProvider>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        var client = Substitute.For<IAmazonDynamoDB>();

        var expected = new List<Dictionary<string, AttributeValue>>
        {
            new() { ["id"] = new AttributeValue { N = "1" } }
        };

        client.QueryAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(new QueryResponse { Items = expected });
        clientProvider.GetDynamoDbClient("connector", 1).Returns(client);

        var model = new StrategyModel<QueryModel>
        {
            Model = new QueryModel { Request = new QueryRequest { TableName = "table" } }
        };

        var sut = new DynamoDbQueryRunner(logger, clientProvider, configProvider);

        var result = await sut.QueryMany(model, "connector", 1, "table");

        Assert.Single(result);
    }

    [Fact]
    public async Task ReadStream_WhenStreamsClientMissing_ThrowsInvalidOperationException()
    {
        var logger = Substitute.For<ILogger<DynamoDbQueryRunner>>();
        var clientProvider = Substitute.For<IDynamoDbClientProvider>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        clientProvider.GetStreamsClient("connector", 1).Returns((AmazonDynamoDBStreamsClient)null);

        var model = new StrategyModel<StreamModel>
        {
            Model = new StreamModel
            {
                StreamArn = "arn:aws:dynamodb:region:acct:table/t/stream/x",
                Request = new GetRecordsRequest { Limit = 10 }
            }
        };

        var sut = new DynamoDbQueryRunner(logger, clientProvider, configProvider);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sut.ReadStream(model, "connector", 1));
    }
}
