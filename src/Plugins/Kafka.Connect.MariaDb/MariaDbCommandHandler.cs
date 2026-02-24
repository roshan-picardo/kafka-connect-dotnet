using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.MariaDb.Models;
using MySqlConnector;

namespace Kafka.Connect.MariaDb;

public interface IMariaDbCommandHandler
{
    Task Initialize(string connector);
    IDictionary<string, Command> Get(string connector);
    JsonNode Next(CommandRecord command, IList<ConnectMessage<JsonNode>> records);
    Task Purge(string connector);
}

public class MariaDbCommandHandler(
    IConfigurationProvider configurationProvider,
    IMariaDbClientProvider mariaDbClientProvider,
    ILogger<MariaDbCommandHandler> logger)
    : IMariaDbCommandHandler
{
    public async Task Initialize(string connector)
    {
        var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
        if (config.Changelog != null)
        {
            try
            {
                var connection = mariaDbClientProvider.GetMariaDbClient(connector).GetConnection();
                using (logger.Track("Making sure audit log table exists"))
                {
                    var lookupLogTable = $"""
                                          SELECT COUNT(*) 
                                          FROM information_schema.tables
                                          WHERE 
                                              table_schema = '{config.Changelog.Schema}' AND 
                                              table_name = '{config.Changelog.Table}'
                                          """;
                    var cmd = new MySqlCommand(lookupLogTable, connection);
                    var exists = Convert.ToInt32(await cmd.ExecuteScalarAsync()) > 0;
                    if (!exists)
                    {
                        var auditLogTable = $"""
                                             CREATE TABLE IF NOT EXISTS {config.Changelog.Schema}.{config.Changelog.Table}
                                             (
                                                 log_id INT AUTO_INCREMENT PRIMARY KEY,
                                                 log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                 log_schema VARCHAR(128),
                                                 log_table VARCHAR(128) NOT NULL,
                                                 log_operation VARCHAR(10) NOT NULL,
                                                 log_before JSON,
                                                 log_after JSON
                                             )
                                             """;
                        await new MySqlCommand(auditLogTable, connection).ExecuteNonQueryAsync();
                    }
                }

                // Create trigger function for MariaDB
                foreach (var (_, command) in config.Commands)
                {
                    using (logger.Track($"Making sure trigger is attached to {command.Schema}.{command.Table}"))
                    {
                        var lookupTrigger = $"""
                                             SELECT COUNT(*)
                                             FROM information_schema.triggers
                                             WHERE 
                                                 trigger_schema = '{command.Schema}' AND
                                                 event_object_table = '{command.Table}' AND
                                                 trigger_name LIKE 'trg_{command.Table}%_audit_log'
                                             """;
                        var cmd = new MySqlCommand(lookupTrigger, connection);
                        var exists = Convert.ToInt32(await cmd.ExecuteScalarAsync()) == 3;
                        if (exists) continue;
                        
                        var columnQuery = $$"""
                                            SELECT  CONCAT('''', COLUMN_NAME, ''',', '{0}.', COLUMN_NAME,  '') 
                                            FROM INFORMATION_SCHEMA.COLUMNS
                                            WHERE TABLE_SCHEMA = '{{command.Schema}}' AND TABLE_NAME = '{{command.Table}}'
                                            """;
                        
                        var columnsCmd = new MySqlCommand(columnQuery, connection);
                        List<string> columns = [];

                        await using (var reader = await columnsCmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                columns.Add(reader.GetString(0));
                            }
                        }

                        // Drop triggers if they exist (to recreate them)
                        var dropInsertTrigger = $"DROP TRIGGER IF EXISTS {command.Schema}.trg_{command.Table}_insert_audit_log";
                        var dropUpdateTrigger = $"DROP TRIGGER IF EXISTS {command.Schema}.trg_{command.Table}_update_audit_log";
                        var dropDeleteTrigger = $"DROP TRIGGER IF EXISTS {command.Schema}.trg_{command.Table}_delete_audit_log";
                        
                        await new MySqlCommand(dropInsertTrigger, connection).ExecuteNonQueryAsync();
                        await new MySqlCommand(dropUpdateTrigger, connection).ExecuteNonQueryAsync();
                        await new MySqlCommand(dropDeleteTrigger, connection).ExecuteNonQueryAsync();

                        // Create insert trigger
                        var insertTrigger = $"""
                                            DROP TRIGGER IF EXISTS {command.Schema}.trg_{command.Table}_insert_audit_log;

                                            CREATE TRIGGER {command.Schema}.trg_{command.Table}_insert_audit_log
                                            AFTER INSERT ON {command.Schema}.{command.Table}
                                            FOR EACH ROW
                                            BEGIN
                                                INSERT INTO {config.Changelog.Schema}.{config.Changelog.Table}
                                                    (log_schema, log_table, log_operation, log_before, log_after)
                                                VALUES
                                                    ('{command.Schema}', '{command.Table}', 'INSERT', NULL, JSON_OBJECT({string.Format(string.Join(',', columns), "NEW")}));
                                            END;
                                            """;
                        await new MySqlCommand(insertTrigger, connection).ExecuteNonQueryAsync();

                        // Create update trigger
                        var updateTrigger = $"""
                                            DROP TRIGGER IF EXISTS {command.Schema}.trg_{command.Table}_update_audit_log;
                                            
                                            CREATE TRIGGER {command.Schema}.trg_{command.Table}_update_audit_log
                                            AFTER UPDATE ON {command.Schema}.{command.Table}
                                            FOR EACH ROW
                                            BEGIN
                                                INSERT INTO {config.Changelog.Schema}.{config.Changelog.Table}
                                                    (log_schema, log_table, log_operation, log_before, log_after)
                                                VALUES
                                                    ('{command.Schema}', '{command.Table}', 'UPDATE', JSON_OBJECT({string.Format(string.Join(',', columns), "OLD")}), JSON_OBJECT({string.Format(string.Join(',', columns), "NEW")}));
                                            END;
                                            """;
                        await new MySqlCommand(updateTrigger, connection).ExecuteNonQueryAsync();

                        // Create delete trigger
                        var deleteTrigger = $"""
                                            DROP TRIGGER IF EXISTS {command.Schema}.trg_{command.Table}_delete_audit_log;
                                            
                                            CREATE TRIGGER {command.Schema}.trg_{command.Table}_delete_audit_log
                                            AFTER DELETE ON {command.Schema}.{command.Table}
                                            FOR EACH ROW
                                            BEGIN
                                                INSERT INTO {config.Changelog.Schema}.{config.Changelog.Table}
                                                    (log_schema, log_table, log_operation, log_before, log_after)
                                                VALUES
                                                    ('{command.Schema}', '{command.Table}', 'DELETE', JSON_OBJECT({string.Format(string.Join(',', columns), "OLD")}), NULL);
                                            END;
                                            """;
                        await new MySqlCommand(deleteTrigger, connection).ExecuteNonQueryAsync();
                    }
                }
            }
            catch (MySqlException exception)
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
                    var connection = mariaDbClientProvider.GetMariaDbClient(connector).GetConnection();
                    var purge = $"""
                                 DELETE FROM {config.Changelog.Schema}.{config.Changelog.Table}
                                 WHERE log_timestamp < DATE_SUB(NOW(), INTERVAL {config.Changelog.Retention} DAY)
                                 """;
                    var records = await new MySqlCommand(purge, connection).ExecuteNonQueryAsync();
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
