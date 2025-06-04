using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.SqlServer.Models;

namespace Kafka.Connect.SqlServer.Strategies;

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
                    var rules = lookup.Take(i).Select(f => $"{f.Key} = '{f.Value}'").ToList();
                    rules.Add($"{lookup.ElementAt(i).Key} > '{lookup.ElementAt(i).Value}'");
                    filters.Add(string.Join(" AND ", rules));
                }

                var where = filters.Count != 0 ? $" WHERE (  {string.Join(" ) OR ( ", filters)} )" : "";
                var orderBy = command.Filters == null ? "" : $" ORDER BY {string.Join(",", command.Filters.Keys)}";
                
                model.Model = $"""
                               SELECT 
                                   ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS id,
                                   'CHANGE' AS operation,
                                   DATEDIFF_BIG(MICROSECOND, '1970-01-01', GETUTCDATE()) AS timestamp,
                                   NULL AS before,
                                   (SELECT * FROM [{command.Schema}].[{command.Table}] WHERE id = t.id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS after
                               FROM [{command.Schema}].[{command.Table}] t
                               {where}
                               {orderBy} 
                               OFFSET 0 ROWS
                               FETCH NEXT {record.BatchSize} ROWS ONLY
                               """;
            }
            else if (command.IsSnapshot())
            {
                if (command.IsInitial())
                {
                    model.Model = $"""
                                   SELECT 
                                       COUNT(*) AS _total, 
                                       DATEDIFF_BIG(MICROSECOND, '1970-01-01', GETUTCDATE()) AS _timestamp 
                                   FROM [{command.Schema}].[{command.Table}];
                                   """;
                }
                else
                {
                    model.Model = string.IsNullOrWhiteSpace(command.Snapshot.Key)
                        ? $"""
                           SELECT 
                               id, 
                               'IMPORT' AS operation,
                               DATEDIFF_BIG(MICROSECOND, '1970-01-01', GETUTCDATE()) AS timestamp,
                               NULL as before,
                               after
                           FROM (
                              SELECT  
                                  ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS id, 
                                  (SELECT * FROM [{command.Schema}].[{command.Table}] WHERE id = t.id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) as after
                              FROM [{command.Schema}].[{command.Table}] t
                           ) AS batch 
                           WHERE id BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize};
                           """
                        : $"""
                           SELECT 
                               {command.Snapshot.Key} AS id, 
                               'IMPORT' AS operation,
                               DATEDIFF_BIG(MICROSECOND, '1970-01-01', GETUTCDATE()) AS timestamp,
                               NULL AS before,
                               (SELECT * FROM [{command.Schema}].[{command.Table}] WHERE id = t.id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS after
                           FROM [{command.Schema}].[{command.Table}] t
                           WHERE id BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize};
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
                                   DATEDIFF_BIG(MICROSECOND, '1970-01-01', log_timestamp) As timestamp,
                                   log_before AS before, 
                                   log_after AS after
                               FROM [{changelog.Schema}].[{changelog.Table}] 
                               WHERE 
                                   log_schema = '{command.Schema}' AND 
                                   log_table = '{command.Table}' AND 
                                   DATEDIFF_BIG(MICROSECOND, '1970-01-01', log_timestamp) >= {command.Snapshot.Timestamp} AND 
                                   log_id > {command.Snapshot.Id}
                               ORDER BY log_timestamp ASC 
                               OFFSET 0 ROWS
                               FETCH NEXT {record.BatchSize} ROWS ONLY
                               """;
            }

            return Task.FromResult(model);
        }
    }
}
