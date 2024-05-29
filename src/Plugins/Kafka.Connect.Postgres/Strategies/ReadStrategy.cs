using System.Text.Json;
using Kafka.Connect.Plugin.Extensions;
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
            var command = record.GetCommand<CommandConfig>();
            var orderBy = $"ORDER BY {command.TimestampColumn} ASC";
            var filters = new List<List<string>>
            {
                new() { $"{command.TimestampColumn} > {command.Timestamp}" },
                new() { $"{command.TimestampColumn} = {command.Timestamp}" },
            };
            if (command.KeyColumns != null)
            {
                const int index = 1;
                foreach (var keyColumn in command.KeyColumns)
                {
                    if (command.Keys?.TryGetValue(keyColumn, out var obj) ?? false)
                    {
                        var value = obj is JsonElement je ? je.GetValue() : obj;
                        if (value is string)
                        {
                            filters.Add([..filters[index], $"{keyColumn} = '{value}'"]);
                            filters[index].Add($"{keyColumn} > '{value}'");
                        }
                        else
                        {
                            filters.Add([..filters[index], $"{keyColumn} = {value}"]);
                            filters[index].Add($"{keyColumn} > {value}");
                        }
                    }

                    orderBy = $"{orderBy}, {keyColumn} ASC";
                }
            }

            filters.RemoveAt(filters.Count - 1);

            var filterBy =
                $"{string.Join(" OR ", filters.Select(f => string.Join(" AND ", f.Select(s => s))))}";

            return Task.FromResult(new StrategyModel<string>
            {
                Status = SinkStatus.Selecting,
                Model =
                    $"SELECT * FROM {command.Schema}.{command.Table} WHERE {filterBy} ORDER BY {orderBy} LIMIT {record.BatchSize}"
            });
        }
    }
}