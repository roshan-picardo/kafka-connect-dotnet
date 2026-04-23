using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MariaDb;
using Kafka.Connect.MariaDb.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using MySqlConnector;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MariaDb;

public class MariaDbPluginHandlerTests
{
    [Fact]
    public async Task Startup_DelegatesToCommandHandler()
    {
        var handler = Substitute.For<IMariaDbCommandHandler>();
        handler.Initialize("c1").Returns(Task.CompletedTask);

        var sut = NewSut(handler: handler);
        await sut.Startup("c1");

        await handler.Received(1).Initialize("c1");
    }

    [Fact]
    public async Task Purge_DelegatesToCommandHandler()
    {
        var handler = Substitute.For<IMariaDbCommandHandler>();
        handler.Purge("c1").Returns(Task.CompletedTask);

        var sut = NewSut(handler: handler);
        await sut.Purge("c1");

        await handler.Received(1).Purge("c1");
    }

    [Fact]
    public void Commands_DelegatesToCommandHandler()
    {
        var handler = Substitute.For<IMariaDbCommandHandler>();
        handler.Get("c1").Returns(new Dictionary<string, Command> { ["r"] = new CommandConfig() });

        var sut = NewSut(handler: handler);
        var result = sut.Commands("c1");

        Assert.Single(result);
    }

    [Fact]
    public void NextCommand_FiltersStatuses()
    {
        var handler = Substitute.For<IMariaDbCommandHandler>();
        handler.Next(Arg.Any<CommandRecord>(), Arg.Any<IList<ConnectMessage<JsonNode>>>()).Returns(JsonNode.Parse("{}"));

        var sut = NewSut(handler: handler);
        var command = new CommandRecord { Command = JsonSerializer.SerializeToNode(new CommandConfig()) };
        var records = new List<ConnectRecord>
        {
            new("t",0,0){ Status = Status.Published, Deserialized = new ConnectMessage<JsonNode>{ Value = JsonNode.Parse("{}") }},
            new("t",0,1){ Status = Status.Skipped, Deserialized = new ConnectMessage<JsonNode>{ Value = JsonNode.Parse("{}") }},
            new("t",0,2){ Status = Status.Triggered, Deserialized = new ConnectMessage<JsonNode>{ Value = JsonNode.Parse("{}") }},
            new("t",0,3){ Status = Status.Failed, Deserialized = new ConnectMessage<JsonNode>{ Value = JsonNode.Parse("{}") }}
        };

        _ = sut.NextCommand(command, records);

        handler.Received(1).Next(command, Arg.Is<IList<ConnectMessage<JsonNode>>>(x => x.Count == 3));
    }

    [Fact]
    public async Task Get_WhenClientConnectionFails_ThrowsConnectDataException()
    {
        var factory = Substitute.For<IConnectPluginFactory>();
        var strategy = Substitute.For<IStrategy>();
        strategy.Build<string>("c1", Arg.Any<IConnectRecord>())
            .Returns(Task.FromResult(new StrategyModel<string> { Status = Status.Selecting, Model = "SELECT 1" }));
        factory.GetStrategy("c1", Arg.Any<IConnectRecord>()).Returns(strategy);

        var clientProvider = Substitute.For<IMariaDbClientProvider>();
        var client = Substitute.For<IMariaDbClient>();
        client.GetConnection().Returns(_ => throw new InvalidOperationException("no connection"));
        clientProvider.GetMariaDbClient("c1", 1).Returns(client);

        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig { Changelog = new ChangelogConfig() });

        var sut = NewSut(configProvider, factory, clientProvider);
        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(new CommandConfig { Keys = ["id"], Topic = "topic" })
        };

        await Assert.ThrowsAsync<ConnectDataException>(() => sut.Get("c1", 1, command));
    }

    [Fact]
    public async Task Put_WhenConnectionFails_ThrowsAfterModeling()
    {
        var factory = Substitute.For<IConnectPluginFactory>();
        var strategy = Substitute.For<IStrategy>();
        strategy.Build<string>("c1", Arg.Any<IConnectRecord>())
            .Returns(Task.FromResult(new StrategyModel<string> { Status = Status.Updating, Model = "UPDATE X" }));
        factory.GetStrategy("c1", Arg.Any<IConnectRecord>()).Returns(strategy);

        var clientProvider = Substitute.For<IMariaDbClientProvider>();
        var client = Substitute.For<IMariaDbClient>();
        client.GetConnection().Returns(_ => throw new InvalidOperationException("no connection"));
        clientProvider.GetMariaDbClient("c1", 1).Returns(client);

        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetParallelRetryOptions("c1").Returns(new ParallelRetryOptions
        {
            DegreeOfParallelism = 1,
            Exceptions = new List<string>(),
            ErrorTolerance = (All: true, Data: false, None: false)
        });

        var sut = NewSut(configProvider, factory, clientProvider);
        var record = new ConnectRecord("topic", 0, 1)
        {
            Status = Status.Processed,
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"id\":1}") }
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => sut.Put([record], "c1", 1));
        Assert.Equal(Status.Updating, record.Status);
    }

    [Fact]
    public async Task Put_WhenNullConnection_MarksSavingRecordAsFailedByRetryPipeline()
    {
        var factory = Substitute.For<IConnectPluginFactory>();
        var strategy = Substitute.For<IStrategy>();
        strategy.Build<string>("c1", Arg.Any<IConnectRecord>())
            .Returns(Task.FromResult(new StrategyModel<string> { Status = Status.Updating, Model = "UPDATE X" }));
        factory.GetStrategy("c1", Arg.Any<IConnectRecord>()).Returns(strategy);

        var clientProvider = Substitute.For<IMariaDbClientProvider>();
        var client = Substitute.For<IMariaDbClient>();
        client.GetConnection().Returns((MySqlConnection)null);
        clientProvider.GetMariaDbClient("c1", 1).Returns(client);

        var sqlExecutor = Substitute.For<IMariaDbSqlExecutor>();
        sqlExecutor.ExecuteNonQueryAsync(Arg.Any<MySqlConnection>(), Arg.Any<string>())
            .Returns(Task.FromException<int>(new Exception("write failed")));

        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetParallelRetryOptions("c1").Returns(new ParallelRetryOptions
        {
            DegreeOfParallelism = 1,
            Exceptions = new List<string>(),
            ErrorTolerance = (All: true, Data: false, None: false)
        });

        var sut = NewSut(configProvider, factory, clientProvider, sqlExecutor: sqlExecutor);
        var record = new ConnectRecord("topic", 0, 1)
        {
            Status = Status.Processed,
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"id\":1}") }
        };

        await sut.Put([record], "c1", 1);

        Assert.Equal(Status.Failed, record.Status);
        Assert.NotNull(record.Exception);
    }

    [Fact]
    public void GetConnectRecord_WhenInitialSnapshot_ReturnsTriggeredWithoutKey()
    {
        var method = typeof(MariaDbPluginHandler).GetMethod("GetConnectRecord", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var command = new CommandRecord
        {
            Changelog = JsonNode.Parse("{}"),
            Command = JsonSerializer.SerializeToNode(new CommandConfig
            {
                Topic = "topic",
                Keys = ["id"],
                Snapshot = new SnapshotConfig { Enabled = true, Total = 0 }
            })
        };

        var message = new Dictionary<string, object>
        {
            ["id"] = 1,
            ["before"] = "{\"id\":1}",
            ["after"] = "{\"id\":1,\"name\":\"Jane\"}"
        };

        var record = (ConnectRecord)method!.Invoke(null, [message, command])!;

        Assert.Equal(Status.Triggered, record.Status);
        Assert.Null(record.Deserialized.Key);
    }

    [Fact]
    public void GetConnectRecord_WhenNormalChange_ParsesBeforeAfterAndBuildsKey()
    {
        var method = typeof(MariaDbPluginHandler).GetMethod("GetConnectRecord", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(new CommandConfig
            {
                Topic = "topic",
                Keys = ["id"],
                Snapshot = new SnapshotConfig { Enabled = false }
            })
        };

        var message = new Dictionary<string, object>
        {
            ["id"] = 10,
            ["before"] = "{\"id\":10,\"name\":\"Old\"}",
            ["after"] = "{\"id\":10,\"name\":\"New\"}"
        };

        var record = (ConnectRecord)method!.Invoke(null, [message, command])!;

        Assert.Equal(Status.Selected, record.Status);
        Assert.NotNull(record.Deserialized.Key);
        Assert.Equal(10, record.Deserialized.Key!["id"]!.GetValue<int>());
        Assert.NotNull(record.Deserialized.Value!["before"]);
        Assert.NotNull(record.Deserialized.Value!["after"]);
    }

    private static MariaDbPluginHandler NewSut(
        IConfigurationProvider configProvider = null,
        IConnectPluginFactory factory = null,
        IMariaDbClientProvider clientProvider = null,
        IMariaDbCommandHandler handler = null,
        IMariaDbSqlExecutor sqlExecutor = null)
    {
        configProvider ??= Substitute.For<IConfigurationProvider>();
        factory ??= Substitute.For<IConnectPluginFactory>();
        clientProvider ??= Substitute.For<IMariaDbClientProvider>();
        handler ??= Substitute.For<IMariaDbCommandHandler>();
        sqlExecutor ??= Substitute.For<IMariaDbSqlExecutor>();
        return new MariaDbPluginHandler(
            configProvider,
            factory,
            handler,
            clientProvider,
            sqlExecutor,
            Substitute.For<ILogger<MariaDbPluginHandler>>());
    }
}
