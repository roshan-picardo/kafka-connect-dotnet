using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Oracle.Models;

namespace Kafka.Connect.Oracle.Strategies;

public class ReadStrategy(ILogger<ReadStrategy> logger) : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
        => throw new NotImplementedException();

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating source models")) 
        {
            var command = record.GetCommand<CommandConfig>();
            var model = new StrategyModel<string> { Status = Status.Selecting};
            if (!record.IsChangeLog())
            {
                List<string> filters = [];
                var lookup = command.Filters?.Where(l => l.Value != null).ToList() ?? [];
                for (var i = 0; i < lookup.Count; i++)
                {
                    var rules = lookup.Take(i).Select(f => $"\"{f.Key}\" = '{f.Value}'").ToList();
                    rules.Add($"\"{lookup.ElementAt(i).Key}\" > '{lookup.ElementAt(i).Value}'");
                    filters.Add(string.Join(" AND ", rules));
                }

                var where = filters.Count != 0 ? $" WHERE (  {string.Join(" ) OR ( ", filters)} )" : "";
                var orderBy = command.Filters == null ? "" : $" ORDER BY {string.Join(",", command.Filters.Keys.Select(k => $"\"{k}\""))}";
                
                model.Model = $"""
                               SELECT
                                   ROWNUM AS "id",
                                   'CHANGE' AS "operation",
                                   (EXTRACT(DAY FROM (SYSTIMESTAMP - TO_TIMESTAMP('1970-01-01', 'YYYY-MM-DD'))) * 86400000000 + 
                                    EXTRACT(HOUR FROM SYSTIMESTAMP) * 3600000000 + 
                                    EXTRACT(MINUTE FROM SYSTIMESTAMP) * 60000000 + 
                                    EXTRACT(SECOND FROM SYSTIMESTAMP) * 1000000) AS "timestamp",
                                   NULL AS "before",
                                   JSON_OBJECT(*) AS "after"
                               FROM {command.Schema}.{command.Table}
                               {where}
                               {orderBy} 
                               FETCH FIRST {record.BatchSize} ROWS ONLY
                               """;

            }
            else if (command.IsSnapshot())
            {
                if (command.IsInitial())
                {
                    model.Model = $"""
                                   SELECT 
                                       COUNT(*) AS "_total", 
                                       (EXTRACT(DAY FROM (SYSTIMESTAMP - TO_TIMESTAMP('1970-01-01', 'YYYY-MM-DD'))) * 86400000000 + 
                                        EXTRACT(HOUR FROM SYSTIMESTAMP) * 3600000000 + 
                                        EXTRACT(MINUTE FROM SYSTIMESTAMP) * 60000000 + 
                                        EXTRACT(SECOND FROM SYSTIMESTAMP) * 1000000) AS "_timestamp" 
                                   FROM {command.Schema}.{command.Table}
                                   """;
                }
                else
                {
                    model.Model = string.IsNullOrWhiteSpace(command.Snapshot.Key)
                        ? $"""
                           SELECT
                               "id",
                               'IMPORT' AS "operation",
                               (EXTRACT(DAY FROM (SYSTIMESTAMP - TO_TIMESTAMP('1970-01-01', 'YYYY-MM-DD'))) * 86400000000 + 
                                EXTRACT(HOUR FROM SYSTIMESTAMP) * 3600000000 + 
                                EXTRACT(MINUTE FROM SYSTIMESTAMP) * 60000000 + 
                                EXTRACT(SECOND FROM SYSTIMESTAMP) * 1000000) AS "timestamp",
                               NULL as "before",
                               "after"
                           FROM (
                              SELECT  
                                  ROWNUM AS "id",
                                  JSON_OBJECT(*) as "after"
                              FROM {command.Schema}.{command.Table}
                           ) batch 
                           WHERE "id" BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize}
                           """
                        : $"""
                           SELECT
                               {command.Snapshot.Key} AS "id",
                               'IMPORT' AS "operation",
                               (EXTRACT(DAY FROM (SYSTIMESTAMP - TO_TIMESTAMP('1970-01-01', 'YYYY-MM-DD'))) * 86400000000 + 
                                EXTRACT(HOUR FROM SYSTIMESTAMP) * 3600000000 + 
                                EXTRACT(MINUTE FROM SYSTIMESTAMP) * 60000000 + 
                                EXTRACT(SECOND FROM SYSTIMESTAMP) * 1000000) AS "timestamp",
                               NULL AS "before",
                               JSON_OBJECT(*) AS "after"
                           FROM {command.Schema}.{command.Table} 
                           WHERE {command.Snapshot.Key} BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize}
                           """;
                }
            }
            else
            {
                var changelog = record.GetChangelog<ChangelogConfig>();
                // Strip quotes from schema and table names for comparison with audit log values
                var schemaName = command.Schema.Trim('"');
                var tableName = command.Table.Trim('"');
                model.Model = $"""
                               SELECT
                                   LOG_ID AS "id",
                                   LOG_OPERATION AS "operation",
                                   (EXTRACT(DAY FROM (LOG_TIMESTAMP - TO_TIMESTAMP('1970-01-01', 'YYYY-MM-DD'))) * 86400000000 +
                                    EXTRACT(HOUR FROM LOG_TIMESTAMP) * 3600000000 +
                                    EXTRACT(MINUTE FROM LOG_TIMESTAMP) * 60000000 +
                                    EXTRACT(SECOND FROM LOG_TIMESTAMP) * 1000000) AS "timestamp",
                                   LOG_BEFORE AS "before",
                                   LOG_AFTER AS "after"
                               FROM {changelog.Schema}.{changelog.Table}
                               WHERE
                                   LOG_SCHEMA='{schemaName}' AND
                                   LOG_TABLE='{tableName}' AND
                                   (EXTRACT(DAY FROM (LOG_TIMESTAMP - TO_TIMESTAMP('1970-01-01', 'YYYY-MM-DD'))) * 86400000000 +
                                    EXTRACT(HOUR FROM LOG_TIMESTAMP) * 3600000000 +
                                    EXTRACT(MINUTE FROM LOG_TIMESTAMP) * 60000000 +
                                    EXTRACT(SECOND FROM LOG_TIMESTAMP) * 1000000) >= {command.Snapshot.Timestamp} AND
                                   LOG_ID > {command.Snapshot.Id}
                               ORDER BY LOG_TIMESTAMP ASC
                               FETCH FIRST {record.BatchSize} ROWS ONLY
                               """;
            }

            return Task.FromResult(model);
        }
    }
}
