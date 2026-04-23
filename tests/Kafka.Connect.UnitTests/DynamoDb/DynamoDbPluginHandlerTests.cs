using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb;
using Kafka.Connect.DynamoDb.Collections;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.DynamoDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.DynamoDb;

public class DynamoDbPluginHandlerTests
{
    [Fact]
    public async Task Get_WithScanStrategy_ReturnsSelectedRecords()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var factory = Substitute.For<IConnectPluginFactory>();
        var queryRunner = Substitute.For<IDynamoDbQueryRunner>();
        var commandHandler = Substitute.For<IDynamoDbCommandHandler>();
        var logger = Substitute.For<ILogger<DynamoDbPluginHandler>>();

        var strategy = new TestScanStrategy();
        factory.GetStrategy("connector", Arg.Any<CommandRecord>()).Returns(strategy);

        var commandConfig = new CommandConfig
        {
            TableName = "table",
            Topic = "topic",
            Keys = new[] { "id" }
        };

        var command = new CommandRecord
        {
            Connector = "connector",
            Name = "read",
            Command = JsonSerializer.SerializeToNode(commandConfig)
        };

        queryRunner.ScanMany(Arg.Any<StrategyModel<ScanModel>>(), "connector", 1, "table")
            .Returns(new List<Dictionary<string, AttributeValue>>
            {
                new()
                {
                    ["id"] = new AttributeValue { N = "1" },
                    ["name"] = new AttributeValue { S = "Jane" }
                }
            });

        var sut = new DynamoDbPluginHandler(configProvider, factory, queryRunner, commandHandler, logger);

        var records = await sut.Get("connector", 1, command);

        Assert.Single(records);
        Assert.Equal(Status.Selected, records[0].Status);
        Assert.NotNull(records[0].Deserialized.Key);
        Assert.Equal("READ", records[0].Deserialized.Value["operation"]!.GetValue<string>());
    }

    [Fact]
    public async Task Get_WithStreamReadStrategy_ReturnsStreamRecords()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var factory = Substitute.For<IConnectPluginFactory>();
        var queryRunner = Substitute.For<IDynamoDbQueryRunner>();
        var commandHandler = Substitute.For<IDynamoDbCommandHandler>();
        var logger = Substitute.For<ILogger<DynamoDbPluginHandler>>();

        var streamStrategy = new TestStreamReadStrategy();
        factory.GetStrategy("connector", Arg.Any<CommandRecord>()).Returns(streamStrategy);

        var commandConfig = new CommandConfig
        {
            TableName = "table",
            Topic = "topic",
            Keys = new[] { "id" }
        };

        var command = new CommandRecord
        {
            Connector = "connector",
            Name = "stream",
            Command = JsonSerializer.SerializeToNode(commandConfig)
        };

        var streamRecord = new Amazon.DynamoDBv2.Model.Record
        {
            EventID = "evt-1",
            EventName = "INSERT",
            Dynamodb = new StreamRecord
            {
                ApproximateCreationDateTime = System.DateTime.UtcNow,
                SequenceNumber = "100",
                NewImage = new Dictionary<string, AttributeValue>
                {
                    ["id"] = new AttributeValue { N = "7" },
                    ["value"] = new AttributeValue { S = "new" }
                }
            }
        };

        queryRunner.ReadStream(Arg.Any<StrategyModel<StreamModel>>(), "connector", 1)
            .Returns(Task.FromResult(((IList<Amazon.DynamoDBv2.Model.Record>)new List<Amazon.DynamoDBv2.Model.Record> { streamRecord }, "next-iterator")));

        var sut = new DynamoDbPluginHandler(configProvider, factory, queryRunner, commandHandler, logger);

        var records = await sut.Get("connector", 1, command);

        Assert.Single(records);
        Assert.Equal(Status.Selected, records[0].Status);
        Assert.Equal("INSERT", records[0].Deserialized.Value["operation"]!.GetValue<string>());
        Assert.Equal("100", records[0].Deserialized.Value["_sequenceNumber"]!.GetValue<string>());
        Assert.Equal("next-iterator", streamStrategy.LastBuiltModel.Model.ShardIterator);
    }

    [Fact]
    public async Task Put_WithSinkingRecords_WritesRequests()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var factory = Substitute.For<IConnectPluginFactory>();
        var queryRunner = Substitute.For<IDynamoDbQueryRunner>();
        var commandHandler = Substitute.For<IDynamoDbCommandHandler>();
        var logger = Substitute.For<ILogger<DynamoDbPluginHandler>>();

        configProvider.GetParallelRetryOptions("connector").Returns(new ParallelRetryOptions
        {
            DegreeOfParallelism = 1,
            Exceptions = new List<string>(),
            ErrorTolerance = (All: true, Data: false, None: false)
        });
        configProvider.GetPluginConfig<PluginConfig>("connector").Returns(new PluginConfig { TableName = "sink-table" });

        var writeStrategy = new TestWriteStrategy();
        factory.GetStrategy("connector", Arg.Any<ConnectRecord>()).Returns(writeStrategy);

        var record = new ConnectRecord("topic", 0, 1)
        {
            Status = Status.Processed,
            Serialized = new ConnectMessage<byte[]> { Key = [1, 2, 3] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Value = JsonNode.Parse("{\"id\":\"1\"}")
            }
        };

        var sut = new DynamoDbPluginHandler(configProvider, factory, queryRunner, commandHandler, logger);

        await sut.Put(new List<ConnectRecord> { record }, "connector", 1);

        await queryRunner.Received(1).WriteMany(Arg.Any<IList<WriteRequest>>(), "connector", 1, "sink-table");
        Assert.Equal(Status.Updating, record.Status);
    }

    [Fact]
    public void Commands_DelegatesToCommandHandler()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var factory = Substitute.For<IConnectPluginFactory>();
        var queryRunner = Substitute.For<IDynamoDbQueryRunner>();
        var commandHandler = Substitute.For<IDynamoDbCommandHandler>();
        var logger = Substitute.For<ILogger<DynamoDbPluginHandler>>();

        commandHandler.Get("connector").Returns(new Dictionary<string, Command>
        {
            ["read"] = new CommandConfig { TableName = "table" }
        });

        var sut = new DynamoDbPluginHandler(configProvider, factory, queryRunner, commandHandler, logger);

        var result = sut.Commands("connector");

        Assert.Single(result);
        Assert.Contains("read", result.Keys);
    }

    [Fact]
    public void NextCommand_UsesPublishedAndSkippedRecordsOnly()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var factory = Substitute.For<IConnectPluginFactory>();
        var queryRunner = Substitute.For<IDynamoDbQueryRunner>();
        var commandHandler = Substitute.For<IDynamoDbCommandHandler>();
        var logger = Substitute.For<ILogger<DynamoDbPluginHandler>>();

        var command = new CommandRecord
        {
            Connector = "connector",
            Name = "read",
            Command = JsonSerializer.SerializeToNode(new CommandConfig())
        };

        commandHandler.Next(Arg.Any<CommandRecord>(), Arg.Any<IList<ConnectMessage<JsonNode>>>())
            .Returns(JsonNode.Parse("{\"next\":true}"));

        var records = new List<ConnectRecord>
        {
            new("topic", 0, 0) { Status = Status.Published, Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{}") } },
            new("topic", 0, 1) { Status = Status.Skipped, Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{}") } },
            new("topic", 0, 2) { Status = Status.Failed, Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{}") } }
        };

        var sut = new DynamoDbPluginHandler(configProvider, factory, queryRunner, commandHandler, logger);

        var result = sut.NextCommand(command, records);

        Assert.NotNull(result);
        commandHandler.Received(1).Next(command, Arg.Is<IList<ConnectMessage<JsonNode>>>(messages => messages.Count == 2));
    }

    private sealed class TestScanStrategy : Strategy<ScanModel>
    {
        protected override Task<StrategyModel<ScanModel>> BuildModels(string connector, ConnectRecord record) =>
            throw new System.NotImplementedException();

        protected override Task<StrategyModel<ScanModel>> BuildModels(string connector, CommandRecord record)
        {
            return Task.FromResult(new StrategyModel<ScanModel>
            {
                Status = Status.Selecting,
                Model = new ScanModel
                {
                    Operation = "SCAN",
                    Request = new ScanRequest { TableName = "table" }
                }
            });
        }
    }

    private sealed class TestWriteStrategy : Strategy<WriteRequest>
    {
        protected override Task<StrategyModel<WriteRequest>> BuildModels(string connector, ConnectRecord record)
        {
            return Task.FromResult(new StrategyModel<WriteRequest>
            {
                Status = Status.Updating,
                Model = new WriteRequest
                {
                    PutRequest = new PutRequest
                    {
                        Item = new Dictionary<string, AttributeValue> { ["id"] = new AttributeValue { N = "1" } }
                    }
                }
            });
        }

        protected override Task<StrategyModel<WriteRequest>> BuildModels(string connector, CommandRecord record) =>
            throw new System.NotImplementedException();
    }

    private sealed class TestStreamReadStrategy : StreamReadStrategy
    {
        public TestStreamReadStrategy() : base(Substitute.For<ILogger<StreamReadStrategy>>())
        {
        }

        public StrategyModel<StreamModel> LastBuiltModel { get; private set; }

        protected override Task<StrategyModel<StreamModel>> BuildModels(string connector, ConnectRecord record) =>
            throw new System.NotImplementedException();

        protected override Task<StrategyModel<StreamModel>> BuildModels(string connector, CommandRecord record)
        {
            LastBuiltModel = new StrategyModel<StreamModel>
            {
                Status = Status.Selecting,
                Model = new StreamModel
                {
                    StreamArn = "arn:aws:dynamodb:region:acct:table/t/stream/x",
                    ShardIterator = "initial",
                    Request = new GetRecordsRequest { Limit = 10 }
                }
            };
            return Task.FromResult(LastBuiltModel);
        }
    }
}
