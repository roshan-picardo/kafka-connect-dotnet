using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Db2.Models;
using IBM.Data.Db2;

namespace Kafka.Connect.Db2;

public interface IDb2CommandHandler
{
    Task Initialize(string connector);
    IDictionary<string, Command> Get(string connector);
    JsonNode Next(CommandRecord command, IList<ConnectMessage<JsonNode>> records);
    Task Purge(string connector);
}

public class Db2CommandHandler(
    IConfigurationProvider configurationProvider,
    IDb2ClientProvider db2ClientProvider,
    IDb2SqlExecutor sqlExecutor,
    ILogger<Db2CommandHandler> logger)
    : IDb2CommandHandler
{
    public async Task Initialize(string connector)
    {
        var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
        if (config.Changelog == null)
        {
            return;
        }

        try
        {
            var connection = db2ClientProvider.GetDb2Client(connector).GetConnection();
            using (logger.Track("Making sure audit log table exists"))
            {
                var lookupLogTable = $"""
                                      SELECT COUNT(*)
                                      FROM SYSCAT.TABLES
                                      WHERE TABSCHEMA = UPPER('{config.Changelog.Schema}')
                                        AND TABNAME = UPPER('{config.Changelog.Table}')
                                      """;
                var exists = Convert.ToInt64(await sqlExecutor.ExecuteScalarAsync(connection, lookupLogTable)) > 0;
                if (!exists)
                {
                    var auditLogTable = $"""
                                         CREATE TABLE {config.Changelog.Schema}.{config.Changelog.Table}
                                         (
                                             LOG_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                             LOG_TIMESTAMP TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
                                             LOG_SCHEMA VARCHAR(128),
                                             LOG_TABLE VARCHAR(128) NOT NULL,
                                             LOG_OPERATION VARCHAR(10) NOT NULL,
                                             LOG_BEFORE CLOB(2M),
                                             LOG_AFTER CLOB(2M)
                                         )
                                         """;
                    await sqlExecutor.ExecuteNonQueryAsync(connection, auditLogTable);
                }
            }

            foreach (var (_, command) in config.Commands)
            {
                using (logger.Track($"Making sure trigger is attached to {command.Schema}.{command.Table}"))
                {
                    var columnsQuery = $"""
                                       SELECT
                                           LISTAGG('KEY ''' || COLNAME || ''' VALUE N."' || COLNAME || '"', ', ')
                                               WITHIN GROUP (ORDER BY COLNO) AS NEW_COLUMNS,
                                           LISTAGG('KEY ''' || COLNAME || ''' VALUE O."' || COLNAME || '"', ', ')
                                               WITHIN GROUP (ORDER BY COLNO) AS OLD_COLUMNS
                                       FROM SYSCAT.COLUMNS
                                       WHERE TABSCHEMA = UPPER('{command.Schema}')
                                         AND TABNAME = UPPER('{command.Table}')
                                       """;

                    var rows = await sqlExecutor.QueryRowsAsync(connection, columnsQuery);
                    var newColumns = rows.Count > 0 && rows[0].TryGetValue("NEW_COLUMNS", out var newCol)
                        ? newCol?.ToString() ?? string.Empty
                        : string.Empty;
                    var oldColumns = rows.Count > 0 && rows[0].TryGetValue("OLD_COLUMNS", out var oldCol)
                        ? oldCol?.ToString() ?? string.Empty
                        : string.Empty;

                    if (string.IsNullOrWhiteSpace(newColumns) || string.IsNullOrWhiteSpace(oldColumns))
                    {
                        logger.Warning($"Table {command.Schema}.{command.Table} does not exist or has no columns. Skipping trigger creation.");
                        continue;
                    }

                    foreach (var (operation, triggerName, transitionClause, beforeValue, afterValue) in BuildTriggers(command, newColumns, oldColumns))
                    {
                        var lookupTrigger = $"""
                                             SELECT COUNT(*)
                                             FROM SYSCAT.TRIGGERS
                                             WHERE TRIGSCHEMA = UPPER('{command.Schema}')
                                               AND TRIGNAME = UPPER('{triggerName}')
                                             """;
                        var exists = Convert.ToInt64(await sqlExecutor.ExecuteScalarAsync(connection, lookupTrigger)) > 0;
                        if (exists)
                        {
                            continue;
                        }

                        var attachTrigger = $"""
                                             CREATE TRIGGER {command.Schema}.{triggerName}
                                             AFTER {operation} ON {command.Schema}.{command.Table}
                                             REFERENCING {transitionClause}
                                             FOR EACH ROW
                                             MODE DB2SQL
                                             BEGIN ATOMIC
                                                 INSERT INTO {config.Changelog.Schema}.{config.Changelog.Table}
                                                     (LOG_SCHEMA, LOG_TABLE, LOG_OPERATION, LOG_BEFORE, LOG_AFTER)
                                                 VALUES
                                                     ('{command.Schema}', '{command.Table}', '{operation}', {beforeValue}, {afterValue});
                                             END
                                             """;
                        await sqlExecutor.ExecuteNonQueryAsync(connection, attachTrigger);
                    }
                }
            }
        }
        catch (DB2Exception exception)
        {
            logger.Critical(
                "Failed to bootstrap audit log - make sure the audit log is enabled on the source tables.",
                exception);
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
        if (config.Changelog is { Table: not null, Retention: > 0 })
        {
            using (logger.Track($"Purging audit log for {config.Changelog.Table} with retention {config.Changelog.Retention} days."))
            {
                try
                {
                    var connection = db2ClientProvider.GetDb2Client(connector).GetConnection();
                    var purge = $"""
                                 DELETE FROM {config.Changelog.Schema}.{config.Changelog.Table}
                                 WHERE LOG_TIMESTAMP < CURRENT TIMESTAMP - {config.Changelog.Retention} DAYS
                                 """;
                    var records = await sqlExecutor.ExecuteNonQueryAsync(connection, purge);
                    logger.Debug($"Purged {records} records from the audit log.");
                }
                catch (Exception exception)
                {
                    logger.Critical("Failed to purge the audit log.", exception);
                }
            }
        }
    }

    private static IEnumerable<(string Operation, string TriggerName, string TransitionClause, string BeforeValue, string AfterValue)> BuildTriggers(
        CommandConfig command,
        string newColumns,
        string oldColumns)
    {
        yield return (
            "INSERT",
            $"TRG_{command.Table.ToUpperInvariant()}_AUDIT_INS",
            "NEW AS N",
            "NULL",
            $"JSON_OBJECT({newColumns})");

        yield return (
            "UPDATE",
            $"TRG_{command.Table.ToUpperInvariant()}_AUDIT_UPD",
            "OLD AS O NEW AS N",
            $"JSON_OBJECT({oldColumns})",
            $"JSON_OBJECT({newColumns})");

        yield return (
            "DELETE",
            $"TRG_{command.Table.ToUpperInvariant()}_AUDIT_DEL",
            "OLD AS O",
            $"JSON_OBJECT({oldColumns})",
            "NULL");
    }
}
