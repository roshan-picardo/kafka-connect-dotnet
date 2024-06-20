using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class ReadStrategy(ILogger<ReadStrategy> logger) : QueryStrategy<string>
{
    protected override Task<StrategyModel<string>> BuildSinkModels(string connector, ConnectRecord record)
    {
        throw new NotImplementedException();
    }

    protected override Task<StrategyModel<string>> BuildSourceModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating source models"))
        {
            var command = record.Get<CommandConfig>();
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
                               SELECT *
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
                                   SELECT COUNT(*) AS _total, EXTRACT(EPOCH FROM current_timestamp) * 1000000 AS _timestamp 
                                   FROM {command.Schema}.{command.Table};
                                   """;
                }
                else
                {
                    model.Model = string.IsNullOrWhiteSpace(command.Snapshot.Key)
                        ? $"""
                           SELECT * 
                           FROM (
                              SELECT  ROW_NUMBER() OVER() AS _row, * 
                              FROM {command.Schema}.{command.Table} 
                           ) AS batch WHERE _row BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize};
                           """
                        : $"""
                           SELECT {command.Snapshot.Key} AS _row, * 
                           FROM {command.Schema}.{command.Table} 
                           WHERE _row BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize};
                           """;
                }
            }
            else
            {
                var changelog = record.Get<Changelog>();
                model.Model = $"""
                               SELECT id, operation, old_value, new_value, EXTRACT(EPOCH FROM updated) * 1000000 timestamp  
                               FROM {changelog.Schema}.{changelog.Table} 
                               WHERE schema_name='{command.Schema}' AND table_name='{command.Table}' AND (EXTRACT(EPOCH FROM updated) * 1000000) >= {command.Snapshot.Timestamp} AND id > {command.Snapshot.Id}
                               ORDER BY updated ASC LIMIT {record.BatchSize}
                               """;
            }

            return Task.FromResult(model);
        }
    }
}
