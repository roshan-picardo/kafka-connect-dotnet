using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class ReadStrategy(ILogger<ReadStrategy> logger) : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildSinkModels(string connector, ConnectRecord record)
    {
        throw new NotImplementedException();
    }

    protected override Task<StrategyModel<string>> BuildSourceModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating source models"))
        {
            var command = record.GetCommand<CommandConfig>();
            var model = new StrategyModel<string>(){ Status = SinkStatus.Selecting};
            if (!record.IsChangeLog())
            {
                List<string> filters = [];
                var lookup = command.Lookup?.Where(l => l.Value != null).ToList() ?? [];
                for (var i = 0; i < lookup.Count; i++)
                {
                    var rules = lookup.Take(i).Select(f => $"{f.Key} = '{f.Value}'").ToList();
                    rules.Add($"{lookup.ElementAt(i).Key} > '{lookup.ElementAt(i).Value}'");
                    filters.Add(string.Join(" AND ", rules));
                }

                var where = filters.Count != 0 ? $"WHERE (  {string.Join(" ) OR ( ", filters)} )" : "";
                var orderBy = string.Join(",", command.Keys);
                
                model.Model = $"""
                               SELECT 
                                   ROW_NUMBER() OVER() AS id,
                                   'CHANGE' AS operation,
                                   (EXTRACT(EPOCH FROM current_timestamp) * 1000000)::bigint AS timestamp,
                                   NULL AS before,
                                   ROW_TO_JSON({command.Table}) AS after
                               FROM {command.Schema}.{command.Table} 
                               {where}
                               ORDER BY {orderBy} 
                               LIMIT {record.BatchSize}
                               """;

            }
            else if (command.IsSnapshot())
            {
                if (command.IsInitial())
                {
                    model.Model = $"""
                                   SELECT 
                                       COUNT(*) AS _total, 
                                       EXTRACT(EPOCH FROM current_timestamp) * 1000000::bigint AS _timestamp 
                                   FROM {command.Schema}.{command.Table};
                                   """;
                }
                else
                {
                    model.Model = string.IsNullOrWhiteSpace(command.Snapshot.Key)
                        ? $"""
                           SELECT 
                               id, 
                               'IMPORT' AS operation,
                               EXTRACT(EPOCH FROM current_timestamp) * 1000000::bigint AS timestamp,
                               NULL as before,
                               after
                           FROM (
                              SELECT  
                                  ROW_NUMBER() OVER() AS id, 
                                  ROW_TO_JSON({command.Table}) as after
                              FROM {command.Schema}.{command.Table} 
                           ) AS batch 
                           WHERE id BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize};
                           """
                        : $"""
                           SELECT 
                               {command.Snapshot.Key} AS id, 
                               'IMPORT' AS operation,
                               (EXTRACT(EPOCH FROM current_timestamp) * 1000000)::bigint AS timestamp,
                               NULL AS before,
                               ROW_TO_JSON({command.Table})::json AS after
                           FROM {command.Schema}.{command.Table} 
                           WHERE id BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize};
                           """;
                }
            }
            else
            {
                var changelog = record.GetChangelog<Changelog>();
                model.Model = $"""
                               SELECT 
                                   log_id AS id, 
                                   log_operation AS operation,
                                   EXTRACT(EPOCH FROM log_timestamp) * 1000000::bigint As timestamp,
                                   log_before AS before, 
                                   log_after AS after
                               FROM {changelog.Schema}.{changelog.Table} 
                               WHERE 
                                   log_schema='{command.Schema}' AND 
                                   log_table='{command.Table}' AND 
                                   (EXTRACT(EPOCH FROM log_timestamp) * 1000000) >= {command.Snapshot.Timestamp} AND 
                                   log_id > {command.Snapshot.Id}
                               ORDER BY log_timestamp ASC 
                               LIMIT {record.BatchSize}
                               """;
            }

            return Task.FromResult(model);
        }
    }
}
