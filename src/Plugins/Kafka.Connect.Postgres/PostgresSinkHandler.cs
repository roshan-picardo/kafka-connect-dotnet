using System.Collections.Concurrent;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Postgres.Models;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Postgres;

public class PostgresSinkHandler : SinkHandler<string>
{
    public PostgresSinkHandler(ILogger<SinkHandler<string>> logger, IWriteStrategyProvider writeStrategyProvider, IConfigurationProvider configurationProvider) : base(logger, writeStrategyProvider, configurationProvider)
    {
    }

    protected override Task Sink(string connector, BlockingCollection<SinkRecord<string>> sinkBatch)
    {
        throw new NotImplementedException();
    }
}