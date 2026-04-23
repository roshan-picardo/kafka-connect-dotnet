using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb;
using Kafka.Connect.MongoDb.Collections;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.MongoDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MongoDb;

public class MongoPluginHandlerTests
{
    [Fact]
    public async Task Startup_And_Purge_Complete()
    {
        var sut = NewSut();

        await sut.Startup("c1");
        await sut.Purge("c1");
    }

    [Fact]
    public void Commands_DelegatesToCommandHandler()
    {
        var commandHandler = Substitute.For<IMongoCommandHandler>();
        commandHandler.Get("c1").Returns(new Dictionary<string, Command> { ["read"] = new CommandConfig() });
        var sut = NewSut(commandHandler: commandHandler);

        var result = sut.Commands("c1");

        Assert.Single(result);
    }

    [Fact]
    public void NextCommand_FiltersStatuses()
    {
        var commandHandler = Substitute.For<IMongoCommandHandler>();
        commandHandler.Next(Arg.Any<CommandRecord>(), Arg.Any<IList<ConnectMessage<JsonNode>>>())
            .Returns(JsonNode.Parse("{}"));

        var sut = NewSut(commandHandler: commandHandler);
        var command = new CommandRecord { Command = JsonSerializer.SerializeToNode(new CommandConfig()) };
        var records = new List<ConnectRecord>
        {
            new("t",0,0){ Status = Status.Published, Deserialized = new ConnectMessage<JsonNode>{ Value = JsonNode.Parse("{}") }},
            new("t",0,1){ Status = Status.Skipped, Deserialized = new ConnectMessage<JsonNode>{ Value = JsonNode.Parse("{}") }},
            new("t",0,2){ Status = Status.Triggered, Deserialized = new ConnectMessage<JsonNode>{ Value = JsonNode.Parse("{}") }},
        };

        _ = sut.NextCommand(command, records);

        commandHandler.Received(1).Next(command, Arg.Is<IList<ConnectMessage<JsonNode>>>(x => x.Count == 2));
    }

    [Fact]
    public async Task Get_ForReadStrategy_ReturnsMappedRecords()
    {
        var strategy = Substitute.For<IStrategy>();
        strategy.Build<FindModel<BsonDocument>>("c1", Arg.Any<IConnectRecord>())
            .Returns(Task.FromResult(new StrategyModel<FindModel<BsonDocument>>
            {
                Status = Status.Selecting,
                Model = new FindModel<BsonDocument> { Operation = "CHANGE" }
            }));

        var factory = Substitute.For<IConnectPluginFactory>();
        factory.GetStrategy("c1", Arg.Any<IConnectRecord>()).Returns(strategy);

        var queryRunner = Substitute.For<IMongoQueryRunner>();
        queryRunner.ReadMany(Arg.Any<StrategyModel<FindModel<BsonDocument>>>(), "c1", 1, "users")
            .Returns(new List<BsonDocument> { BsonDocument.Parse("{\"id\":1,\"name\":\"Jane\"}") });

        var sut = NewSut(factory: factory, queryRunner: queryRunner);
        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(new CommandConfig { Collection = "users", Keys = ["id"], Topic = "topic" })
        };

        var records = await sut.Get("c1", 1, command);

        Assert.Single(records);
        Assert.Equal(Status.Selected, records[0].Status);
        Assert.NotNull(records[0].Deserialized.Value);
    }

    [Fact]
    public async Task Get_ForStreamStrategy_ReturnsFromWatchMany()
    {
        var streamStrategy = new StreamsReadStrategy(Substitute.For<ILogger<StreamsReadStrategy>>());
        var factory = Substitute.For<IConnectPluginFactory>();
        factory.GetStrategy("c1", Arg.Any<IConnectRecord>()).Returns(streamStrategy);

        var queryRunner = Substitute.For<IMongoQueryRunner>();
        queryRunner.WatchMany(Arg.Any<StrategyModel<WatchModel>>(), "c1", 1, "users")
            .Returns(new List<ChangeStreamDocument<BsonDocument>>());

        var sut = NewSut(factory: factory, queryRunner: queryRunner);
        var command = new CommandRecord
        {
            BatchSize = 10,
            Command = JsonSerializer.SerializeToNode(new CommandConfig { Collection = "users", Topic = "topic" })
        };

        var records = await sut.Get("c1", 1, command);

        Assert.Empty(records);
    }

    [Fact]
    public async Task Put_BuildsModelsAndCallsWriteMany()
    {
        var strategy = Substitute.For<IStrategy>();
        strategy.Build<WriteModel<BsonDocument>>("c1", Arg.Any<IConnectRecord>())
            .Returns(Task.FromResult(new StrategyModel<WriteModel<BsonDocument>>
            {
                Status = Status.Updating,
                Models = [new ReplaceOneModel<BsonDocument>(Builders<BsonDocument>.Filter.Empty, new BsonDocument("id", 1))]
            }));

        var factory = Substitute.For<IConnectPluginFactory>();
        factory.GetStrategy("c1", Arg.Any<IConnectRecord>()).Returns(strategy);

        var queryRunner = Substitute.For<IMongoQueryRunner>();
        var configProvider = BuildConfigProvider();

        var sut = NewSut(configProvider: configProvider, factory: factory, queryRunner: queryRunner);
        var record = new ConnectRecord("topic", 0, 1)
        {
            Status = Status.Processed,
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"id\":1}") }
        };

        await sut.Put([record], "c1", 1);

        Assert.Equal(Status.Updating, record.Status);
        await queryRunner.Received(1).WriteMany(Arg.Is<IList<WriteModel<BsonDocument>>>(m => m.Count == 1), "c1", 1);
    }

    private static IConfigurationProvider BuildConfigProvider()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>(Arg.Any<string>()).Returns(new PluginConfig());
        configProvider.GetParallelRetryOptions(Arg.Any<string>()).Returns(new ParallelRetryOptions
        {
            DegreeOfParallelism = 1,
            Exceptions = new List<string>(),
            ErrorTolerance = (All: true, Data: false, None: false)
        });
        return configProvider;
    }

    private static MongoPluginHandler NewSut(
        IConfigurationProvider configProvider = null,
        IConnectPluginFactory factory = null,
        IMongoQueryRunner queryRunner = null,
        IMongoCommandHandler commandHandler = null)
    {
        configProvider ??= BuildConfigProvider();
        factory ??= Substitute.For<IConnectPluginFactory>();
        queryRunner ??= Substitute.For<IMongoQueryRunner>();
        commandHandler ??= Substitute.For<IMongoCommandHandler>();

        return new MongoPluginHandler(
            configProvider,
            factory,
            queryRunner,
            commandHandler,
            Substitute.For<ILogger<MongoPluginHandler>>());
    }
}
