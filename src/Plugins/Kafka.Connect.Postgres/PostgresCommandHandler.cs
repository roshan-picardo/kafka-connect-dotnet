using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Postgres.Models;
using Npgsql;

namespace Kafka.Connect.Postgres;

public interface IPostgresCommandHandler
{
    Task Initialize(string connector);
    IDictionary<string, Command> Get(string connector);
    JsonNode Next(CommandRecord command, IList<ConnectMessage<JsonNode>> records);
}

public class PostgresCommandHandler(
    IConfigurationProvider configurationProvider,
    IPostgresClientProvider postgresClientProvider,
    ILogger<PostgresCommandHandler> logger) : IPostgresCommandHandler
{
    public async Task Initialize(string connector)
    {
        var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
        if (config.Changelog != null)
        {
            try
            {
                var connection = postgresClientProvider.GetPostgresClient(connector).GetConnection();
                using (logger.Track("Making sure audit log table exists"))
                {
                    var lookupLogTable = $"""
                                          SELECT COUNT(*) 
                                          FROM information_schema.tables
                                          WHERE 
                                              table_catalog = '{config.Database}' AND
                                              table_schema = '{config.Changelog.Schema}' AND 
                                              table_name = '{config.Changelog.Table}';
                                          """;
                    var exists = (long)(await new NpgsqlCommand(lookupLogTable, connection).ExecuteScalarAsync())! > 0;
                    if (!exists)
                    {
                        var auditLogTable = $"""
                                             CREATE TABLE IF NOT EXISTS {config.Changelog.Schema}.{config.Changelog.Table}
                                             (
                                                 log_id serial,
                                                 log_timestamp timestamp  default current_timestamp,
                                                 log_schema text,
                                                 log_table text NOT NULL,
                                                 log_operation text NOT NULL,
                                                 log_before json,
                                                 log_after json,
                                                 CONSTRAINT "pk_{config.Changelog.Table}" PRIMARY KEY (log_id),
                                                 CONSTRAINT "uk_{config.Changelog.Table}" UNIQUE (log_id)
                                             ); 
                                             """;
                        await new NpgsqlCommand(auditLogTable, connection).ExecuteScalarAsync();
                    }
                }

                using (logger.Track("Making sure audit log function exists"))
                {
                    var lookupLogFunction = $"""
                                             SELECT COUNT(*)
                                             FROM information_schema.routines
                                             WHERE 
                                                 routine_catalog = '{config.Database}' AND
                                                 routine_schema = '{config.Changelog.Schema}' AND 
                                                 routine_name = 'trg_func_connect_audit_logger'
                                             """;
                    var exists = (long)(await new NpgsqlCommand(lookupLogFunction, connection).ExecuteScalarAsync())! >
                                 0;
                    if (!exists)
                    {
                        var auditLogTrigger = $"""
                                               CREATE FUNCTION trg_func_connect_audit_logger() RETURNS TRIGGER AS $audit_log$
                                                   BEGIN
                                                       INSERT INTO {config.Changelog.Schema}.{config.Changelog.Table}(log_schema, log_table, log_operation, log_before, log_after) 
                                                       VALUES(tg_table_schema, tg_table_name, tg_op, row_to_json(OLD.*), row_to_json(NEW.*));
                                                       RETURN NULL;
                                                   END; 
                                               $audit_log$ LANGUAGE plpgsql;
                                               """;
                        await new NpgsqlCommand(auditLogTrigger, connection).ExecuteScalarAsync();
                    }
                }

                foreach (var (_, command) in config.Commands)
                {
                    using (logger.Track($"Making sure trigger is attached to {command.Schema}.{command.Table}"))
                    {
                        var lookupTrigger = $"""
                                             SELECT COUNT(*)
                                             FROM information_schema.triggers
                                             WHERE 
                                                 trigger_catalog = '{config.Database}' AND
                                                 trigger_schema = '{command.Schema}' AND 
                                                 trigger_name = 'trg_{command.Table}_audit_log' AND
                                                 event_object_table = '{command.Table}' 
                                             """;
                        var exists = (long)(await new NpgsqlCommand(lookupTrigger, connection).ExecuteScalarAsync())! >
                                     0;
                        if (exists) continue;
                        var attachTrigger = $"""
                                             CREATE OR REPLACE TRIGGER trg_{command.Table}_audit_log
                                             AFTER INSERT OR UPDATE OR DELETE ON {command.Schema}.{command.Table} 
                                             FOR EACH ROW EXECUTE PROCEDURE trg_func_connect_audit_logger();
                                             """;
                        await new NpgsqlCommand(attachTrigger, connection).ExecuteScalarAsync();
                    }
                }
            }
            catch (PostgresException exception)
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
}
