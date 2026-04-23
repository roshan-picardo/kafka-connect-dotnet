using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MySql;
using Kafka.Connect.MySql.Models;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using MySql.Data.MySqlClient;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MySql;

public class MySqlCommandHandlerTests
{
    [Fact]
    public void Get_ReturnsConfiguredCommands()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Commands = new Dictionary<string, CommandConfig>
            {
                ["read"] = new() { Table = "users" }
            }
        });

        var sut = NewSut(configProvider: configProvider);

        var result = sut.Get("c1");

        Assert.Single(result);
    }

    [Fact]
    public void Next_ForSourceUpdatesFilters()
    {
        var sut = NewSut();
        var config = new CommandConfig { Filters = new Dictionary<string, object> { ["id"] = 0 } };
        var command = new CommandRecord { Command = JsonSerializer.SerializeToNode(config) };
        var records = new List<ConnectMessage<JsonNode>>
        {
            new() { Value = JsonNode.Parse("{\"after\":{\"id\":5,\"name\":\"a\"}}") }
        };

        var next = sut.Next(command, records);

        Assert.NotNull(next);
        Assert.Equal(5, next!["Filters"]!["id"]!.GetValue<int>());
    }

    [Fact]
    public void Next_ForSourceWithNoRecords_LeavesFiltersUnchanged()
    {
        var sut = NewSut();
        var config = new CommandConfig { Filters = new Dictionary<string, object> { ["id"] = 1 } };
        var command = new CommandRecord { Command = JsonSerializer.SerializeToNode(config) };

        var next = sut.Next(command, []);

        Assert.Equal(1, next!["Filters"]!["id"]!.GetValue<int>());
    }

    [Fact]
    public void Next_ForInitialSnapshot_SetsTotalAndTimestamp()
    {
        var sut = NewSut();
        var config = new CommandConfig { Snapshot = new SnapshotConfig { Enabled = true, Total = 0 } };
        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(config),
            Changelog = JsonNode.Parse("{}")
        };
        var records = new List<ConnectMessage<JsonNode>>
        {
            new() { Value = JsonNode.Parse("{\"_total\":7,\"_timestamp\":99}") }
        };

        var next = sut.Next(command, records);

        Assert.Equal(7, next!["Snapshot"]!["Total"]!.GetValue<long>());
        Assert.Equal(99, next["Snapshot"]!["Timestamp"]!.GetValue<long>());
    }

    [Fact]
    public void Next_ForSnapshotBatch_DisablesSnapshotWhenComplete()
    {
        var sut = NewSut();
        var config = new CommandConfig
        {
            Snapshot = new SnapshotConfig { Enabled = true, Total = 5, Id = 3 }
        };
        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(config),
            Changelog = JsonNode.Parse("{}")
        };
        var records = new List<ConnectMessage<JsonNode>>
        {
            new() { Value = JsonNode.Parse("{\"id\":5}") }
        };

        var next = sut.Next(command, records);

        Assert.False(next!["Snapshot"]!["Enabled"]!.GetValue<bool>());
        Assert.Equal(0, next["Snapshot"]!["Id"]!.GetValue<long>());
        Assert.Equal(-1, next["Snapshot"]!["Total"]!.GetValue<long>());
    }

    [Fact]
    public void Next_ForChangelogMode_UpdatesSnapshotCursor()
    {
        var sut = NewSut();
        var config = new CommandConfig
        {
            Snapshot = new SnapshotConfig { Enabled = false, Timestamp = 0, Id = 0 }
        };
        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(config),
            Changelog = JsonNode.Parse("{}")
        };
        var records = new List<ConnectMessage<JsonNode>>
        {
            new() { Value = JsonNode.Parse("{\"id\":10,\"timestamp\":1234}") }
        };

        var next = sut.Next(command, records);

        Assert.Equal(10, next!["Snapshot"]!["Id"]!.GetValue<long>());
        Assert.Equal(1234, next["Snapshot"]!["Timestamp"]!.GetValue<long>());
    }

    [Fact]
    public async Task Initialize_WhenNoChangelog_Completes()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig { Changelog = null });
        var sut = NewSut(configProvider: configProvider);

        await sut.Initialize("c1");
    }

    [Fact]
    public async Task Initialize_WhenChangelogAndConnectionFails_IsHandled()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Changelog = new ChangelogConfig { Schema = "log", Table = "audit" },
            Commands = new Dictionary<string, CommandConfig>
            {
                ["read"] = new() { Schema = "dbo", Table = "users" }
            }
        });

        var clientProvider = Substitute.For<IMySqlClientProvider>();
        clientProvider.GetMySqlClient("c1", -1).Returns(new FailingMySqlClient());

        var sut = NewSut(configProvider, clientProvider);

        await sut.Initialize("c1");
    }

    [Fact]
    public async Task Initialize_WhenAuditObjectsMissing_CreatesTableAndTriggers()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Changelog = new ChangelogConfig { Schema = "audit", Table = "log" },
            Commands = new Dictionary<string, CommandConfig>
            {
                ["read"] = new() { Schema = "dbo", Table = "users" }
            }
        });

        var clientProvider = Substitute.For<IMySqlClientProvider>();
        var client = Substitute.For<IMySqlClient>();
        client.GetConnection().Returns(new MySqlConnection());
        clientProvider.GetMySqlClient("c1", -1).Returns(client);

        var sql = Substitute.For<IMySqlSqlExecutor>();
        sql.ExecuteScalarAsync(Arg.Any<MySqlConnection>(), Arg.Any<string>()).Returns(0, 0);
        sql.QuerySingleColumnAsync(Arg.Any<MySqlConnection>(), Arg.Any<string>())
            .Returns(new List<string> { "'id',NEW.id", "'name',NEW.name" });

        var sut = NewSut(configProvider, clientProvider, sql);

        await sut.Initialize("c1");

        await sql.Received(2).ExecuteScalarAsync(Arg.Any<MySqlConnection>(), Arg.Any<string>());
        await sql.Received(7).ExecuteNonQueryAsync(Arg.Any<MySqlConnection>(), Arg.Any<string>());
        await sql.Received(1).QuerySingleColumnAsync(Arg.Any<MySqlConnection>(), Arg.Any<string>());
    }

    [Fact]
    public async Task Initialize_WhenTriggersAlreadyExist_SkipsTriggerBootstrap()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Changelog = new ChangelogConfig { Schema = "audit", Table = "log" },
            Commands = new Dictionary<string, CommandConfig>
            {
                ["read"] = new() { Schema = "dbo", Table = "users" }
            }
        });

        var clientProvider = Substitute.For<IMySqlClientProvider>();
        var client = Substitute.For<IMySqlClient>();
        client.GetConnection().Returns(new MySqlConnection());
        clientProvider.GetMySqlClient("c1", -1).Returns(client);

        var sql = Substitute.For<IMySqlSqlExecutor>();
        sql.ExecuteScalarAsync(Arg.Any<MySqlConnection>(), Arg.Any<string>()).Returns(1, 3);

        var sut = NewSut(configProvider, clientProvider, sql);

        await sut.Initialize("c1");

        await sql.DidNotReceive().QuerySingleColumnAsync(Arg.Any<MySqlConnection>(), Arg.Any<string>());
    }

    [Fact]
    public async Task Purge_WhenNoChangelog_DoesNothing()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var clientProvider = Substitute.For<IMySqlClientProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig { Changelog = null });
        var sut = NewSut(configProvider, clientProvider);

        await sut.Purge("c1");

        clientProvider.DidNotReceive().GetMySqlClient(Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact]
    public async Task Purge_WhenConnectionFails_IsSwallowed()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var clientProvider = Substitute.For<IMySqlClientProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Changelog = new ChangelogConfig { Schema = "log", Table = "audit", Retention = 1 }
        });
        clientProvider.GetMySqlClient("c1", -1).Returns(_ => throw new Exception("db down"));
        var sut = NewSut(configProvider, clientProvider);

        await sut.Purge("c1");
    }

    [Fact]
    public async Task Purge_WhenRetentionNotPositive_DoesNothing()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var clientProvider = Substitute.For<IMySqlClientProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Changelog = new ChangelogConfig { Schema = "log", Table = "audit", Retention = 0 }
        });
        var sut = NewSut(configProvider, clientProvider);

        await sut.Purge("c1");

        clientProvider.DidNotReceive().GetMySqlClient(Arg.Any<string>(), Arg.Any<int>());
    }

    private static MySqlCommandHandler NewSut(
        IConfigurationProvider configProvider = null,
        IMySqlClientProvider clientProvider = null,
        IMySqlSqlExecutor sqlExecutor = null)
    {
        configProvider ??= Substitute.For<IConfigurationProvider>();
        clientProvider ??= Substitute.For<IMySqlClientProvider>();
        sqlExecutor ??= Substitute.For<IMySqlSqlExecutor>();

        return new MySqlCommandHandler(
            configProvider,
            clientProvider,
            sqlExecutor,
            Substitute.For<ILogger<MySqlCommandHandler>>());
    }

    private sealed class FailingMySqlClient : IMySqlClient
    {
        public string ApplicationName => "failing";

        public MySqlConnection GetConnection()
        {
            var conn = new MySqlConnection("Server=127.0.0.1;Port=1;Database=db;Uid=u;Pwd=p;Connection Timeout=1;");
            conn.Open();
            return conn;
        }
    }
}
