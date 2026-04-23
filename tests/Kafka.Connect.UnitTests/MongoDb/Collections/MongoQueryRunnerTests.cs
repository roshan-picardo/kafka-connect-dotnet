using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Collections;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MongoDb.Collections;

public class MongoQueryRunnerTests
{
    [Fact]
    public async Task WriteMany_WhenModelsNull_CompletesWithoutCallingClient()
    {
        var clientProvider = Substitute.For<IMongoClientProvider>();
        var sut = NewSut(clientProvider: clientProvider);

        await sut.WriteMany(null, "c1", 1);

        clientProvider.DidNotReceive().GetMongoClient(Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact]
    public async Task WriteMany_WhenModelsEmpty_CompletesWithoutCallingClient()
    {
        var clientProvider = Substitute.For<IMongoClientProvider>();
        var sut = NewSut(clientProvider: clientProvider);

        await sut.WriteMany(new List<WriteModel<BsonDocument>>(), "c1", 1);

        clientProvider.DidNotReceive().GetMongoClient(Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact]
    public async Task WriteMany_WhenMongoException_ThrowsConnectRetriableException()
    {
        var clientProvider = Substitute.For<IMongoClientProvider>();
        var client = Substitute.For<IMongoClient>();
        client.GetDatabase(Arg.Any<string>(), Arg.Any<MongoDatabaseSettings>()).Returns(_ => throw new MongoException("db error"));
        clientProvider.GetMongoClient("c1", 1).Returns(client);

        var sut = NewSut(clientProvider: clientProvider);
        var models = new List<WriteModel<BsonDocument>>
        {
            new InsertOneModel<BsonDocument>(new BsonDocument("id", 1))
        };

        await Assert.ThrowsAsync<ConnectRetriableException>(() => sut.WriteMany(models, "c1", 1));
    }

    [Fact]
    public async Task ReadMany_WhenProviderThrows_Throws()
    {
        var clientProvider = Substitute.For<IMongoClientProvider>();
        clientProvider.GetMongoClient("c1", 1).Returns(_ => throw new InvalidOperationException("missing"));
        var sut = NewSut(clientProvider: clientProvider);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sut.ReadMany(
            new StrategyModel<FindModel<BsonDocument>>
            {
                Model = new FindModel<BsonDocument>
                {
                    Filter = Builders<BsonDocument>.Filter.Empty,
                    Options = new FindOptions<BsonDocument>()
                }
            }, "c1", 1, "users"));
    }

    [Fact]
    public async Task WatchMany_WhenProviderThrows_Throws()
    {
        var clientProvider = Substitute.For<IMongoClientProvider>();
        clientProvider.GetMongoClient("c1", 1).Returns(_ => throw new InvalidOperationException("missing"));
        var sut = NewSut(clientProvider: clientProvider);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sut.WatchMany(
            new StrategyModel<WatchModel>
            {
                Model = new WatchModel { Options = new ChangeStreamOptions { BatchSize = 5 } }
            }, "c1", 1, "users"));
    }

    private static MongoQueryRunner NewSut(
        IMongoClientProvider clientProvider = null,
        IConfigurationProvider configProvider = null)
    {
        clientProvider ??= Substitute.For<IMongoClientProvider>();
        configProvider ??= Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>(Arg.Any<string>()).Returns(new PluginConfig
        {
            Database = "db",
            Collection = "users",
            IsWriteOrdered = true
        });

        return new MongoQueryRunner(
            Substitute.For<ILogger<MongoQueryRunner>>(),
            clientProvider,
            configProvider);
    }
}
