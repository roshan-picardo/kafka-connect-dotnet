using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Postgres;
using Kafka.Connect.Postgres.Models;
using Npgsql;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Postgres;

public class PostgresCommandHandlerTests
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
                ["read"] = new() { Schema = "public", Table = "users" }
            }
        });

        var clientProvider = Substitute.For<IPostgresClientProvider>();
        var client = Substitute.For<IPostgresClient>();
        client.GetConnection().Returns(new NpgsqlConnection());
        clientProvider.GetPostgresClient("c1", -1).Returns(client);

        var sqlExecutor = Substitute.For<IPostgresSqlExecutor>();
        sqlExecutor.ExecuteScalarAsync(Arg.Any<NpgsqlConnection>(), Arg.Any<string>()).Returns(0L);

        var sut = NewSut(configProvider, clientProvider, sqlExecutor);

        await sut.Initialize("c1");

        await sqlExecutor.Received().ExecuteScalarAsync(
            Arg.Any<NpgsqlConnection>(),
            Arg.Is<string>(sql => sql.Contains("CREATE TABLE IF NOT EXISTS audit.log")));
        await sqlExecutor.Received().ExecuteScalarAsync(
            Arg.Any<NpgsqlConnection>(),
            Arg.Is<string>(sql => sql.Contains("CREATE FUNCTION audit.trg_func_log_audit_logger")));
        await sqlExecutor.Received().ExecuteScalarAsync(
            Arg.Any<NpgsqlConnection>(),
            Arg.Is<string>(sql => sql.Contains("CREATE OR REPLACE TRIGGER trg_users_audit_log")));
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
                ["read"] = new() { Schema = "public", Table = "users" }
            }
        });

        var clientProvider = Substitute.For<IPostgresClientProvider>();
        var client = Substitute.For<IPostgresClient>();
        client.GetConnection().Returns(new NpgsqlConnection());
        clientProvider.GetPostgresClient("c1", -1).Returns(client);

        var sqlExecutor = Substitute.For<IPostgresSqlExecutor>();
        sqlExecutor.ExecuteScalarAsync(Arg.Any<NpgsqlConnection>(), Arg.Any<string>()).Returns(1L, 1L, 1L);

        var sut = NewSut(configProvider, clientProvider, sqlExecutor);

        await sut.Initialize("c1");

        await sqlExecutor.DidNotReceive().ExecuteScalarAsync(Arg.Any<NpgsqlConnection>(), Arg.Is<string>(sql => sql.Contains("CREATE OR REPLACE TRIGGER")));
    }

    [Fact]
    public async Task Purge_WhenNoChangelog_DoesNothing()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var clientProvider = Substitute.For<IPostgresClientProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig { Changelog = null });
        var sut = NewSut(configProvider, clientProvider);

        await sut.Purge("c1");

        clientProvider.DidNotReceive().GetPostgresClient(Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact]
    public async Task Purge_WhenConnectionFails_IsSwallowed()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var clientProvider = Substitute.For<IPostgresClientProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Changelog = new ChangelogConfig { Schema = "audit", Table = "log", Retention = 1 }
        });
        clientProvider.GetPostgresClient("c1", -1).Returns(_ => throw new Exception("db down"));

        var sut = NewSut(configProvider, clientProvider);

        await sut.Purge("c1");
    }

    [Fact]
    public async Task Purge_WhenRetentionNotPositive_DoesNothing()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        var clientProvider = Substitute.For<IPostgresClientProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Changelog = new ChangelogConfig { Schema = "audit", Table = "log", Retention = 0 }
        });
        var sut = NewSut(configProvider, clientProvider);

        await sut.Purge("c1");

        clientProvider.DidNotReceive().GetPostgresClient(Arg.Any<string>(), Arg.Any<int>());
    }

    private static PostgresCommandHandler NewSut(
        IConfigurationProvider configProvider = null,
        IPostgresClientProvider clientProvider = null,
        IPostgresSqlExecutor sqlExecutor = null)
    {
        configProvider ??= Substitute.For<IConfigurationProvider>();
        clientProvider ??= Substitute.For<IPostgresClientProvider>();
        sqlExecutor ??= Substitute.For<IPostgresSqlExecutor>();

        return new PostgresCommandHandler(
            configProvider,
            clientProvider,
            sqlExecutor,
            Substitute.For<ILogger<PostgresCommandHandler>>());
    }
}