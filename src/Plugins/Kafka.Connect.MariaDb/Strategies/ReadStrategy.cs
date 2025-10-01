using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.MariaDb.Models;

namespace Kafka.Connect.MariaDb.Strategies;

public class ReadStrategy(ILogger<ReadStrategy> logger)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
        => throw new NotImplementedException();

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating read SQL"))
        {
            var config = record.GetCommand<CommandConfig>();
            var model = new StrategyModel<string> { Status = Status.Selecting };
            
            if (!record.IsChangeLog())
            {
                var where = config.Filters?.Count > 0
                    ? $"WHERE {string.Join(" AND ", config.Filters.Select(f => $"`{f.Key}` = '{f.Value}'"))}"
                    : string.Empty;
                
                model.Model = $"""
                              SELECT *
                              FROM `{config.Schema}`.`{config.Table}`
                              {where}
                              LIMIT {record.BatchSize}
                              """;
            }
            else if (config.IsSnapshot())
            {
                if (config.IsInitial())
                {
                    model.Model = $"""
                                  SELECT
                                      COUNT(*) AS _total,
                                      UNIX_TIMESTAMP(NOW()) AS _timestamp
                                  FROM `{config.Schema}`.`{config.Table}`
                                  """;
                }
                else
                {
                    var changeLog = record.GetChangelog<ChangelogConfig>();
                    var where = config.Snapshot.Id > 0
                        ? $"WHERE log_id > {config.Snapshot.Id}"
                        : string.Empty;
                    
                    model.Model = $"""
                                  SELECT
                                      log_id AS id,
                                      log_operation AS operation,
                                      log_before AS before,
                                      log_after AS after
                                  FROM `{changeLog.Schema}`.`{changeLog.Table}`
                                  {where}
                                  ORDER BY log_id
                                  LIMIT {record.BatchSize}
                                  """;
                }
            }
            else
            {
                var changeLog = record.GetChangelog<ChangelogConfig>();
                var where = config.Snapshot.Timestamp > 0
                    ? $"WHERE UNIX_TIMESTAMP(log_timestamp) > {config.Snapshot.Timestamp}"
                    : string.Empty;
                
                model.Model = $"""
                              SELECT
                                  log_id AS id,
                                  UNIX_TIMESTAMP(log_timestamp) AS timestamp,
                                  log_operation AS operation,
                                  log_before AS before,
                                  log_after AS after
                              FROM `{changeLog.Schema}`.`{changeLog.Table}`
                              {where}
                              ORDER BY log_timestamp
                              LIMIT {record.BatchSize}
                              """;
            }
            
            return Task.FromResult(model);
        }
    }
}
