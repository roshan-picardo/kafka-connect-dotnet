using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.SqlServer.Models;
using Microsoft.Data.SqlClient;

namespace Kafka.Connect.SqlServer;

public interface ISqlServerCommandHandler
{
    Task Initialize(string connector);
    IDictionary<string, Command> Get(string connector);
    JsonNode Next(CommandRecord command, IList<ConnectMessage<JsonNode>> records);
    Task Purge(string connector);
}

public class SqlServerCommandHandler(
    IConfigurationProvider configurationProvider,
    ISqlServerClientProvider sqlServerClientProvider,
    ILogger<SqlServerCommandHandler> logger)
    : ISqlServerCommandHandler
{
    public async Task Initialize(string connector)
    {
        var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
        if (config.Changelog != null)
        {
            try
            {
                var connection = sqlServerClientProvider.GetSqlServerClient(connector).GetConnection();
                using (logger.Track("Making sure audit log table exists"))
                {
                    var lookupLogTable = $"""
                                          SELECT COUNT(*) 
                                          FROM INFORMATION_SCHEMA.TABLES
                                          WHERE 
                                              TABLE_CATALOG = '{config.Database}' AND
                                              TABLE_SCHEMA = '{config.Changelog.Schema}' AND 
                                              TABLE_NAME = '{config.Changelog.Table}';
                                          """;
                    var exists = (int)(await new SqlCommand(lookupLogTable, connection).ExecuteScalarAsync())! > 0;
                    if (!exists)
                    {
                        var auditLogTable = $"""
                                             CREATE TABLE [{config.Changelog.Schema}].[{config.Changelog.Table}]
                                             (
                                                 log_id INT IDENTITY(1,1) PRIMARY KEY,
                                                 log_timestamp DATETIME DEFAULT GETDATE(),
                                                 log_schema NVARCHAR(128),
                                                 log_table NVARCHAR(128) NOT NULL,
                                                 log_operation NVARCHAR(10) NOT NULL,
                                                 log_before NVARCHAR(MAX),
                                                 log_after NVARCHAR(MAX),
                                                 CONSTRAINT [uk_{config.Changelog.Table}] UNIQUE (log_id)
                                             );
                                             """;
                        await new SqlCommand(auditLogTable, connection).ExecuteNonQueryAsync();
                    }
                }

                foreach (var (_, command) in config.Commands)
                {
                    using (logger.Track($"Making sure trigger is attached to {command.Schema}.{command.Table}"))
                    {
                        var lookupTrigger = $"""
                                             SELECT COUNT(*)
                                             FROM sys.triggers t
                                             JOIN sys.tables tab ON t.parent_id = tab.object_id
                                             JOIN sys.schemas s ON tab.schema_id = s.schema_id
                                             WHERE 
                                                 s.name = '{command.Schema}' AND 
                                                 tab.name = '{command.Table}' AND
                                                 t.name = 'trg_{command.Table}_audit_log'
                                             """;
                        var exists = (int)(await new SqlCommand(lookupTrigger, connection).ExecuteScalarAsync())! > 0;
                        if (exists) continue;
                        
                        var attachTrigger = $"""
                                             CREATE TRIGGER [trg_{command.Table}_audit_log]
                                             ON [{command.Schema}].[{command.Table}]
                                             AFTER INSERT, UPDATE, DELETE
                                             AS
                                             BEGIN
                                                 SET NOCOUNT ON;
                                                 
                                                 DECLARE @operation NVARCHAR(10);
                                                 
                                                 IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
                                                     SET @operation = 'UPDATE';
                                                 ELSE IF EXISTS (SELECT * FROM inserted)
                                                     SET @operation = 'INSERT';
                                                 ELSE IF EXISTS (SELECT * FROM deleted)
                                                     SET @operation = 'DELETE';
                                                 
                                                 INSERT INTO [{config.Changelog.Schema}].[{config.Changelog.Table}]
                                                     (log_schema, log_table, log_operation, log_before, log_after)
                                                 SELECT
                                                     '{command.Schema}',
                                                     '{command.Table}',
                                                     @operation,
                                                     CASE 
                                                         WHEN @operation IN ('UPDATE', 'DELETE') THEN (SELECT * FROM deleted FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
                                                         ELSE NULL
                                                     END,
                                                     CASE 
                                                         WHEN @operation IN ('UPDATE', 'INSERT') THEN (SELECT * FROM inserted FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
                                                         ELSE NULL
                                                     END;
                                             END;
                                             """;
                        await new SqlCommand(attachTrigger, connection).ExecuteNonQueryAsync();
                    }
                }
            }
            catch (SqlException exception)
            {
                logger.Critical(
                    "Failed to bootstrap audit log - make sure the audit log is enabled on the source tables.",
                    exception);
            }
        }
    }

    public IDictionary<string, Command> Get(string connector)
    {
        var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
        return config.Commands.ToDictionary(k => k.Key, v => v.Value as Command);
    }

    public JsonNode Next(CommandRecord command, IList<ConnectMessage<JsonNode>> records)
    {
        var config = command.GetCommand<CommandConfig>();
        if (!command.IsChangeLog())
        {
            if (records.Count > 0)
            {
                var sorted = records.Select(r => r.Value["after"].ToDictionary("after", true)).OrderBy(_ => 1);
                foreach (var key in config.Filters.Keys)
                {
                    sorted = sorted.ThenBy(d => d[key]);
                }

                config.Filters = sorted.LastOrDefault()?.Where(x => config.Filters.Keys.Contains(x.Key))
                    .ToDictionary();
            }
        }
        else if (config.IsSnapshot())
        {
            if (config.IsInitial())
            {
                var value = records.Single().Convert().Value;
                if (value.TryGetValue("_total", out var total))
                {
                    config.Snapshot.Total = Convert.ToInt64(total);
                }

                if (value.TryGetValue("_timestamp", out var timestamp))
                {
                    config.Snapshot.Timestamp = Convert.ToInt64(timestamp);
                }
            }
            else
            {
                if (records.Count > 0)
                {
                    config.Snapshot.Id = records.Max(m => m.Value["id"]!.GetValue<long>());
                }

                if (config.Snapshot.Total >= config.Snapshot.Id)
                {
                    config.Snapshot.Enabled = false;
                    config.Snapshot.Id = 0;
                    config.Snapshot.Total = -1;
                }
            }
        }
        else
        {
            if (records.Count > 0)
            {
                config.Snapshot.Timestamp = (long)records.Max(m => m.Value["timestamp"]!.GetValue<double>());
                config.Snapshot.Id = records.Max(m => m.Value["id"]!.GetValue<long>());
            }
        }

        return config.ToJson();
    }

    public async Task Purge(string connector)
    {
        var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
        if(config.Changelog is { Table: not null, Retention: > 0 })
        {
            using (logger.Track($"Purging audit log for {config.Changelog.Table} with retention {config.Changelog.Retention} days."))
            {
                try
                {
                    var connection = sqlServerClientProvider.GetSqlServerClient(connector).GetConnection();
                    var purge = $"""
                                 DELETE FROM [{config.Changelog.Schema}].[{config.Changelog.Table}]
                                 WHERE log_timestamp < DATEADD(day, -{config.Changelog.Retention}, GETDATE());
                                 """;
                    var records = await new SqlCommand(purge, connection).ExecuteNonQueryAsync();
                    logger.Debug($"Purged {records} records from the audit log.");
                }
                catch (Exception exception)
                {
                    logger.Critical("Failed to purge the audit log.", exception);
                }
            }
        }
    }
}
