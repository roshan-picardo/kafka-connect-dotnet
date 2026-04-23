using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.SqlServer;
using Kafka.Connect.SqlServer.Models;
using Microsoft.Data.SqlClient;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.SqlServer;

public class SqlServerCommandHandlerTests
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
    public async Task Initialize_WhenAuditObjectsMissing_CreatesTableAndTriggers()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Database = "appdb",
            Changelog = new ChangelogConfig { Schema = "audit", Table = "log" },
            Commands = new Dictionary<string, CommandConfig>
            {
                ["read"] = new() { Schema = "dbo", Table = "users" }
            }
        });

        var clientProvider = Substitute.For<ISqlServerClientProvider>();
        var client = Substitute.For<ISqlServerClient>();
        client.GetConnection().Returns(new SqlConnection());
        clientProvider.GetSqlServerClient("c1").Returns(client);

        var sqlExecutor = Substitute.For<ISqlServerSqlExecutor>();
        sqlExecutor.ExecuteScalarAsync(Arg.Any<SqlConnection>(), Arg.Any<string>())
            .Returns((object)0);

        var sut = NewSut(configProvider, clientProvider, sqlExecutor);

        await sut.Initialize("c1");

        await sqlExecutor.Received().ExecuteNonQueryAsync(
            Arg.Any<SqlConnection>(),
            Arg.Is<string>(sql => sql.Contains("CREATE TABLE [audit].[log]")));
        await sqlExecutor.Received().ExecuteNonQueryAsync(
            Arg.Any<SqlConnection>(),
            Arg.Is<string>(sql => sql.Contains("CREATE TRIGGER [trg_users_audit_log]")));
    }

    [Fact]
    public async Task Initialize_WhenTriggersAlreadyExist_SkipsTriggerBootstrap()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Database = "appdb",
            Changelog = new ChangelogConfig { Schema = "audit", Table = "log" },
            Commands = new Dictionary<string, CommandConfig>
            {
                ["read"] = new() { Schema = "dbo", Table = "users" }
            }
        });

        var clientProvider = Substitute.For<ISqlServerClientProvider>();
        var client = Substitute.For<ISqlServerClient>();
        client.GetConnection().Returns(new SqlConnection());
        clientProvider.GetSqlServerClient("c1").Returns(client);

        var sqlExecutor = Substitute.For<ISqlServerSqlExecutor>();
        // table exists (1), trigger exists (1)
        sqlExecutor.ExecuteScalarAsync(Arg.Any<SqlConnection>(), Arg.Any<string>())
            .Returns((object)1, (object)1);

        var sut = NewSut(configProvider, clientProvider, sqlExecutor);

        await sut.Initialize("c1");

        await sqlExecutor.DidNotReceive().ExecuteNonQueryAsync(
            Arg.Any<SqlConnection>(),
            Arg.Is<string>(sql => sql.Contains("CREATE TRIGGER")));
    }

    [Fact]
    public async Task Purge_WhenNoChangelog_DoesNothing()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var clientProvider = Substitute.For<ISqlServerClientProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig { Changelog = null });
        var sut = NewSut(configProvider, clientProvider);

        await sut.Purge("c1");

        clientProvider.DidNotReceive().GetSqlServerClient(Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact]
    public async Task Purge_WhenConnectionFails_IsSwallowed()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var clientProvider = Substitute.For<ISqlServerClientProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Changelog = new ChangelogConfig { Schema = "audit", Table = "log", Retention = 1 }
        });
        clientProvider.GetSqlServerClient("c1", -1).Returns(_ => throw new Exception("db down"));

        var sut = NewSut(configProvider, clientProvider);

        await sut.Purge("c1");
    }

    [Fact]
    public async Task Purge_WhenRetentionNotPositive_DoesNothing()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var clientProvider = Substitute.For<ISqlServerClientProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Changelog = new ChangelogConfig { Schema = "audit", Table = "log", Retention = 0 }
        });
        var sut = NewSut(configProvider, clientProvider);

        await sut.Purge("c1");

        clientProvider.DidNotReceive().GetSqlServerClient(Arg.Any<string>(), Arg.Any<int>());
    }

    private static SqlServerCommandHandler NewSut(
        IConfigurationProvider configProvider = null,
        ISqlServerClientProvider clientProvider = null,
        ISqlServerSqlExecutor sqlExecutor = null)
    {
        configProvider ??= Substitute.For<IConfigurationProvider>();
        clientProvider ??= Substitute.For<ISqlServerClientProvider>();
        sqlExecutor ??= Substitute.For<ISqlServerSqlExecutor>();

        return new SqlServerCommandHandler(
            configProvider,
            clientProvider,
            sqlExecutor,
            Substitute.For<ILogger<SqlServerCommandHandler>>());
    }
}
