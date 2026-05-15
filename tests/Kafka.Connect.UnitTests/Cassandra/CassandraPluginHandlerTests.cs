using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Cassandra;
using Kafka.Connect.Cassandra;
using Kafka.Connect.Cassandra.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Cassandra;

public class CassandraPluginHandlerTests
{
    [Fact]
    public async Task Startup_DelegatesToCommandHandler()
    {
        var handler = Substitute.For<ICassandraCommandHandler>();
        handler.Initialize("c1").Returns(Task.CompletedTask);

        var sut = NewSut(handler: handler);
        await sut.Startup("c1");

        await handler.Received(1).Initialize("c1");
    }

    [Fact]
    public async Task Purge_DelegatesToCommandHandler()
    {
        var handler = Substitute.For<ICassandraCommandHandler>();
        handler.Purge("c1").Returns(Task.CompletedTask);

        var sut = NewSut(handler: handler);
        await sut.Purge("c1");

        await handler.Received(1).Purge("c1");
    }

    [Fact]
    public void Commands_DelegatesToCommandHandler()
    {
        var handler = Substitute.For<ICassandraCommandHandler>();
        handler.Get("c1").Returns(new Dictionary<string, Command> { ["r"] = new CommandConfig() });

        var sut = NewSut(handler: handler);
        var result = sut.Commands("c1");

        Assert.Single(result);
    }

    [Fact]
    public async Task Get_WhenReadFails_ThrowsConnectDataException()
    {
        var factory = Substitute.For<IConnectPluginFactory>();
        var strategy = Substitute.For<IStrategy>();
        strategy.Build<string>("c1", Arg.Any<IConnectRecord>())
            .Returns(Task.FromResult(new StrategyModel<string> { Status = Status.Selecting, Model = "SELECT * FROM ks.t" }));
        factory.GetStrategy("c1", Arg.Any<IConnectRecord>()).Returns(strategy);

        var clientProvider = Substitute.For<ICassandraClientProvider>();
        var client = Substitute.For<ICassandraClient>();
        client.GetSession().Returns(Substitute.For<ISession>());
        clientProvider.GetCassandraClient("c1", 1).Returns(client);

        var sqlExecutor = Substitute.For<ICassandraSqlExecutor>();
        sqlExecutor.QueryRowsAsync(Arg.Any<ISession>(), Arg.Any<string>())
            .Returns(Task.FromException<IList<Dictionary<string, object>>>(new InvalidOperationException("bad read")));

        var configProvider = Substitute.For<IConfigurationProvider>();
        var sut = NewSut(configProvider, factory, clientProvider, sqlExecutor);
        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(new CommandConfig { Keys = ["id"], Topic = "topic" })
        };

        await Assert.ThrowsAsync<ConnectDataException>(() => sut.Get("c1", 1, command));
    }

    [Fact]
    public async Task Put_WhenModelBuilt_SetsRecordStatus()
    {
        var factory = Substitute.For<IConnectPluginFactory>();
        var strategy = Substitute.For<IStrategy>();
        strategy.Build<string>("c1", Arg.Any<IConnectRecord>())
            .Returns(Task.FromResult(new StrategyModel<string> { Status = Status.Updating, Model = "INSERT INTO ks.t (id) VALUES ('1')" }));
        factory.GetStrategy("c1", Arg.Any<IConnectRecord>()).Returns(strategy);

        var clientProvider = Substitute.For<ICassandraClientProvider>();
        var client = Substitute.For<ICassandraClient>();
        var session = Substitute.For<ISession>();
        client.GetSession().Returns(session);
        clientProvider.GetCassandraClient("c1", 1).Returns(client);

        var sqlExecutor = Substitute.For<ICassandraSqlExecutor>();
        sqlExecutor.ExecuteAsync(session, Arg.Any<string>()).Returns(Task.FromResult((RowSet)null!));

        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetParallelRetryOptions("c1").Returns(new ParallelRetryOptions
        {
            DegreeOfParallelism = 1,
            Exceptions = new List<string>(),
            ErrorTolerance = (All: true, Data: false, None: false)
        });

        var sut = NewSut(configProvider, factory, clientProvider, sqlExecutor);
        var record = new ConnectRecord("topic", 0, 1)
        {
            Status = Status.Processed,
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"id\":\"1\"}") }
        };

        await sut.Put([record], "c1", 1);

        Assert.Equal(Status.Updating, record.Status);
    }

    private static CassandraPluginHandler NewSut(
        IConfigurationProvider configProvider = null,
        IConnectPluginFactory factory = null,
        ICassandraClientProvider clientProvider = null,
        ICassandraSqlExecutor sqlExecutor = null,
        ICassandraCommandHandler handler = null)
    {
        configProvider ??= Substitute.For<IConfigurationProvider>();
        factory ??= Substitute.For<IConnectPluginFactory>();
        clientProvider ??= Substitute.For<ICassandraClientProvider>();
        sqlExecutor ??= Substitute.For<ICassandraSqlExecutor>();
        handler ??= Substitute.For<ICassandraCommandHandler>();

        return new CassandraPluginHandler(
            configProvider,
            factory,
            handler,
            clientProvider,
            sqlExecutor,
            Substitute.For<ILogger<CassandraPluginHandler>>());
    }
}
