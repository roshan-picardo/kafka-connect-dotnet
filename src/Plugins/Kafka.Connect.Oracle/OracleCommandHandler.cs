using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Oracle.Models;
using Oracle.ManagedDataAccess.Client;

namespace Kafka.Connect.Oracle;

public interface IOracleCommandHandler
{
    Task Initialize(string connector);
    IDictionary<string, Command> Get(string connector);
    JsonNode Next(CommandRecord command, IList<ConnectMessage<JsonNode>> records);
    Task Purge(string connector);
}

public class OracleCommandHandler(
    IConfigurationProvider configurationProvider,
    IOracleClientProvider oracleClientProvider,
    IOracleSqlExecutor sqlExecutor,
    ILogger<OracleCommandHandler> logger)
    : IOracleCommandHandler
{
    public async Task Initialize(string connector)
    {
        var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
        if (config.Changelog != null)
        {
            try
            {
                var connection = oracleClientProvider.GetOracleClient(connector).GetConnection();
                using (logger.Track("Making sure audit log table exists"))
                {
                    // Strip quotes from schema and table names for metadata lookup
                    var changelogSchema = config.Changelog.Schema.Trim('"');
                    var changelogTable = config.Changelog.Table.Trim('"');
                    var lookupLogTable = $"""
                                          SELECT COUNT(*)
                                          FROM ALL_TABLES
                                          WHERE
                                              OWNER = '{changelogSchema}' AND
                                              TABLE_NAME = '{changelogTable}'
                                          """;
                    var exists = Convert.ToInt32(await sqlExecutor.ExecuteScalarAsync(connection, lookupLogTable)) > 0;
                    if (!exists)
                    {
                        var auditLogTable = $"""
                                             CREATE TABLE {config.Changelog.Schema}.{config.Changelog.Table}
                                             (
                                                 LOG_ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                 LOG_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP,
                                                 LOG_SCHEMA VARCHAR2(128),
                                                 LOG_TABLE VARCHAR2(128) NOT NULL,
                                                 LOG_OPERATION VARCHAR2(10) NOT NULL,
                                                 LOG_BEFORE CLOB,
                                                 LOG_AFTER CLOB
                                             )
                                             """;
                        await sqlExecutor.ExecuteNonQueryAsync(connection, auditLogTable);
                    }
                }

                // No helper function needed - we'll build JSON directly in the trigger

                foreach (var (_, command) in config.Commands)
                {
                    using (logger.Track($"Making sure trigger is attached to {command.Schema}.{command.Table}"))
                    {
                        // Strip quotes from schema and table names for metadata lookup
                        var schemaName = command.Schema.Trim('"');
                        var tableName = command.Table.Trim('"');
                        var lookupTrigger = $"""
                                             SELECT COUNT(*)
                                             FROM ALL_TRIGGERS
                                             WHERE
                                                 OWNER = '{schemaName}' AND
                                                 TABLE_NAME = '{tableName}' AND
                                                 TRIGGER_NAME = 'TRG_{tableName.ToUpper()}_AUDIT_LOG'
                                             """;
                        var exists = Convert.ToInt32(await sqlExecutor.ExecuteScalarAsync(connection, lookupTrigger)) > 0;
                        if (exists) continue;

                        // Execute the LISTAGG query in C# to get column lists
                        // Need to quote column names in trigger references to preserve case
                        // Use column_name as-is to preserve camelCase in JSON keys
                        var columnsQuery = $"""
                                          SELECT
                                              LISTAGG('''' || column_name || ''' VALUE :NEW."' || column_name || '"', ', ')
                                              WITHIN GROUP (ORDER BY column_id) AS new_columns,
                                              LISTAGG('''' || column_name || ''' VALUE :OLD."' || column_name || '"', ', ')
                                              WITHIN GROUP (ORDER BY column_id) AS old_columns
                                          FROM ALL_TAB_COLUMNS
                                          WHERE TABLE_NAME = '{tableName}'
                                          AND OWNER = '{schemaName}'
                                          """;
                        
                        var rows = await sqlExecutor.QueryRowsAsync(connection, columnsQuery);
                        var newColumns = string.Empty;
                        var oldColumns = string.Empty;

                        if (rows.Count > 0)
                        {
                            var row = rows[0];
                            if (row.TryGetValue("new_columns", out var newCol))
                            {
                                newColumns = newCol?.ToString() ?? string.Empty;
                            }
                            if (row.TryGetValue("old_columns", out var oldCol))
                            {
                                oldColumns = oldCol?.ToString() ?? string.Empty;
                            }
                        }
                        
                        // Skip trigger creation if table doesn't exist or has no columns
                        if (string.IsNullOrEmpty(newColumns) || string.IsNullOrEmpty(oldColumns))
                        {
                            logger.Warning($"Table {command.Schema}.{command.Table} does not exist or has no columns. Skipping trigger creation.");
                            continue;
                        }
                        
                        var attachTrigger = $"""
                                             CREATE OR REPLACE TRIGGER {command.Schema}.TRG_{tableName.ToUpper()}_AUDIT_LOG
                                             AFTER INSERT OR UPDATE OR DELETE ON {command.Schema}.{command.Table}
                                             FOR EACH ROW
                                             DECLARE
                                                 v_operation VARCHAR2(10);
                                                 v_before CLOB;
                                                 v_after CLOB;
                                             BEGIN
                                                 IF INSERTING THEN
                                                     v_operation := 'INSERT';
                                                     v_before := NULL;
                                                     SELECT JSON_OBJECT({newColumns}) INTO v_after FROM DUAL;
                                                 ELSIF UPDATING THEN
                                                     v_operation := 'UPDATE';
                                                     SELECT JSON_OBJECT({oldColumns}) INTO v_before FROM DUAL;
                                                     SELECT JSON_OBJECT({newColumns}) INTO v_after FROM DUAL;
                                                 ELSIF DELETING THEN
                                                     v_operation := 'DELETE';
                                                     SELECT JSON_OBJECT({oldColumns}) INTO v_before FROM DUAL;
                                                     v_after := NULL;
                                                 END IF;
                                                 
                                                 INSERT INTO {config.Changelog.Schema}.{config.Changelog.Table}
                                                     (LOG_SCHEMA, LOG_TABLE, LOG_OPERATION, LOG_BEFORE, LOG_AFTER)
                                                 VALUES
                                                     ('{schemaName}', '{tableName}', v_operation, v_before, v_after);
                                             END;
                                             """;
                        await sqlExecutor.ExecuteNonQueryAsync(connection, attachTrigger);
                    }
                }
            }
            catch (OracleException exception)
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
                    var connection = oracleClientProvider.GetOracleClient(connector).GetConnection();
                    var purge = $"""
                                 DELETE FROM {config.Changelog.Schema}.{config.Changelog.Table}
                                 WHERE LOG_TIMESTAMP < SYSTIMESTAMP - INTERVAL '{config.Changelog.Retention}' DAY
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
}
