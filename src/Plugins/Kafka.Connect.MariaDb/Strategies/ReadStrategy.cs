using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.MariaDb.Models;

namespace Kafka.Connect.MariaDb.Strategies;

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
                    var rules = lookup.Take(i).Select(f => $"`{f.Key}` = '{f.Value}'").ToList();
                    rules.Add($"`{lookup.ElementAt(i).Key}` > '{lookup.ElementAt(i).Value}'");
                    filters.Add(string.Join(" AND ", rules));
                }

                var where = filters.Count != 0 ? $" WHERE (  {string.Join(" ) OR ( ", filters)} )" : "";
                var orderBy = command.Filters == null ? "" : $" ORDER BY {string.Join(",", command.Filters.Keys.Select(k => $"`{k}`"))}";
                
                model.Model = $"""
                               SELECT 
                                   @row_number:=@row_number+1 AS id,
                                   'CHANGE' AS operation,
                                   UNIX_TIMESTAMP(NOW(6)) * 1000000 AS timestamp,
                                   NULL AS `before`,
                                   JSON_OBJECT(*) AS `after`
                               FROM {command.Schema}.{command.Table}, (SELECT @row_number:=0) AS r
                               {where}
                               {orderBy} 
                               LIMIT {record.BatchSize}
                               """;

            }
            else if (command.IsSnapshot())
            {
                if (command.IsInitial())
                {
                    model.Model = $"""
                                   SELECT 
                                       COUNT(*) AS "_total", 
                                       UNIX_TIMESTAMP(NOW(6)) * 1000000 AS "_timestamp" 
                                   FROM {command.Schema}.{command.Table}
                                   """;
                }
                else
                {
                    model.Model = string.IsNullOrWhiteSpace(command.Snapshot.Key)
                        ? $"""
                           SELECT 
                               id, 
                               'IMPORT' AS operation,
                               UNIX_TIMESTAMP(NOW(6)) * 1000000 AS timestamp,
                               NULL as `before`,
                               after
                           FROM (
                              SELECT  
                                  @row_number:=@row_number+1 AS id, 
                                  JSON_OBJECT(*) as `after`
                              FROM {command.Schema}.{command.Table}, (SELECT @row_number:=0) AS r
                           ) batch 
                           WHERE id BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize}
                           """
                        : $"""
                           SELECT 
                               {command.Snapshot.Key} AS id, 
                               'IMPORT' AS operation,
                               UNIX_TIMESTAMP(NOW(6)) * 1000000 AS timestamp,
                               NULL AS `before`,
                               JSON_OBJECT(*) AS `after`
                           FROM {command.Schema}.{command.Table} 
                           WHERE {command.Snapshot.Key} BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize}
                           """;
                }
            }
            else
            {
                var changelog = record.GetChangelog<ChangelogConfig>();
                model.Model = $"""
                               SELECT 
                                   log_id AS id, 
                                   log_operation AS operation,
                                   UNIX_TIMESTAMP(log_timestamp) * 1000000 AS timestamp,
                                   log_before AS `before`, 
                                   log_after AS `after`
                               FROM {changelog.Schema}.{changelog.Table}
                               WHERE 
                                   log_schema='{command.Schema}' AND 
                                   log_table='{command.Table}' AND 
                                   UNIX_TIMESTAMP(log_timestamp) * 1000000 >= {command.Snapshot.Timestamp} AND 
                                   log_id > {command.Snapshot.Id}
                               ORDER BY log_timestamp ASC 
                               LIMIT {record.BatchSize}
                               """;
            }

            return Task.FromResult(model);
        }
    }
}
