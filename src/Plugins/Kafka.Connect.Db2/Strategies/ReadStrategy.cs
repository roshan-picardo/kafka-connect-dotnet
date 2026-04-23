using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Db2.Models;

namespace Kafka.Connect.Db2.Strategies;

public class ReadStrategy(ILogger<ReadStrategy> logger) : Strategy<string>
{
    private const string EpochCurrentTimestamp = "((BIGINT(DAYS(CURRENT TIMESTAMP)) - BIGINT(DAYS(TIMESTAMP('1970-01-01-00.00.00')))) * 86400000) + (MIDNIGHT_SECONDS(CURRENT TIMESTAMP) * 1000) + INT(MICROSECOND(CURRENT TIMESTAMP) / 1000)";
    private const string EpochLogTimestamp = "((BIGINT(DAYS(LOG_TIMESTAMP)) - BIGINT(DAYS(TIMESTAMP('1970-01-01-00.00.00')))) * 86400000) + (MIDNIGHT_SECONDS(LOG_TIMESTAMP) * 1000) + INT(MICROSECOND(LOG_TIMESTAMP) / 1000)";

    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
        => throw new NotImplementedException();

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating source models"))
        {
            var command = record.GetCommand<CommandConfig>();
            var model = new StrategyModel<string> { Status = Status.Selecting };
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

                var where = filters.Count != 0 ? $" WHERE (  {string.Join(" ) OR ( ", filters)} )" : string.Empty;
                var orderBy = command.Filters == null ? string.Empty : $" ORDER BY {string.Join(",", command.Filters.Keys.Select(key => $"\"{key}\""))}";

                model.Model = $"""
                               SELECT
                                   BIGINT(ROW_NUMBER() OVER(ORDER BY 1)) AS id,
                                   'CHANGE' AS operation,
                                   {EpochCurrentTimestamp} AS timestamp,
                                   CAST(NULL AS CLOB(2M)) AS before,
                                   JSON_OBJECT(*) AS after
                               FROM {command.Schema}.{command.Table} t
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
                                       COUNT(*) AS _total,
                                       {EpochCurrentTimestamp} AS _timestamp
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
                               {EpochCurrentTimestamp} AS timestamp,
                               CAST(NULL AS CLOB(2M)) AS before,
                               after
                           FROM (
                               SELECT
                                   BIGINT(ROW_NUMBER() OVER(ORDER BY 1)) AS id,
                                   JSON_OBJECT(*) AS after
                               FROM {command.Schema}.{command.Table} t
                           ) AS batch
                           WHERE id BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize}
                           """
                        : $"""
                           SELECT
                               "{command.Snapshot.Key}" AS id,
                               'IMPORT' AS operation,
                               {EpochCurrentTimestamp} AS timestamp,
                               CAST(NULL AS CLOB(2M)) AS before,
                               JSON_OBJECT(*) AS after
                           FROM {command.Schema}.{command.Table} t
                           WHERE "{command.Snapshot.Key}" BETWEEN {command.Snapshot.Id + 1} AND {command.Snapshot.Id + record.BatchSize}
                           """;
                }
            }
            else
            {
                var changelog = record.GetChangelog<ChangelogConfig>();
                model.Model = $"""
                               SELECT
                                   LOG_ID AS id,
                                   LOG_OPERATION AS operation,
                                   {EpochLogTimestamp} AS timestamp,
                                   LOG_BEFORE AS before,
                                   LOG_AFTER AS after
                               FROM {changelog.Schema}.{changelog.Table}
                               WHERE
                                   LOG_SCHEMA = '{command.Schema}' AND
                                   LOG_TABLE = '{command.Table}' AND
                                   {EpochLogTimestamp} >= {command.Snapshot.Timestamp} AND
                                   LOG_ID > {command.Snapshot.Id}
                               ORDER BY LOG_TIMESTAMP ASC
                               FETCH NEXT {record.BatchSize} ROWS ONLY
                               """;
            }

            return Task.FromResult(model);
        }
    }
}
