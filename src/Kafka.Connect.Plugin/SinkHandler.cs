using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Plugin;

public abstract class SinkHandler<TModel> : ISinkHandler
{
    private readonly ILogger<SinkHandler<TModel>> _logger;
    private readonly IWriteStrategyProvider _writeStrategyProvider;
    private readonly IConfigurationProvider _configurationProvider;

    protected SinkHandler(
        ILogger<SinkHandler<TModel>> logger,
        IWriteStrategyProvider writeStrategyProvider,
        IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _writeStrategyProvider = writeStrategyProvider;
        _configurationProvider = configurationProvider;
    }

    public async Task<SinkRecordBatch> Put(SinkRecordBatch batches, string connector, int taskId, int parallelism = 100)
    {
        using (_logger.Track("Invoking Put"))
        {
            connector ??= batches.Connector;
            var sinkBatch = new BlockingCollection<SinkRecord<TModel>>();
            foreach (var batch in batches.GetByTopicPartition<SinkRecord>())
            {
                using (LogContext.Push(new PropertyEnricher("topic", batch.Topic),
                           new PropertyEnricher("partition", batch.Partition)))
                {
                    await batch.Batch.ForEachAsync(async record =>
                    {
                        using (LogContext.Push(new PropertyEnricher("offset", record.Offset)))
                        {
                            if (record.IsSaved)
                            {
                                _logger.Debug("Record already saved to mongodb.");
                                return;
                            }

                            var sinkRecord = new SinkRecord<TModel>(record);

                            if (!record.Skip)
                            {
                                var strategy = _writeStrategyProvider.GetWriteStrategy(connector, record);
                                if (strategy == null)
                                {
                                    sinkRecord.Status = SinkStatus.Failed;
                                    // lets throw retriable as some of the messages might pass on last attempt
                                    throw new ConnectRetriableException("Local_WriteStrategy",
                                            new NullReferenceException(
                                                "Failed to load the Write Model Strategy. Check if the strategy is registered and configured for this record."))
                                        .SetLogContext(record);
                                }

                                (sinkRecord.Status, sinkRecord.Models) = await strategy.BuildModels<TModel>(connector, record);
                                _logger.Trace("Write Models created successfully", new { Strategy = strategy.GetType().FullName });
                            }
                            else
                            {
                                sinkRecord.Status = SinkStatus.Skipping;
                            }

                            _logger.Trace(
                                sinkRecord.Ready
                                    ? "Write models created successfully."
                                    : "Sink record marked for skipping.",
                                new
                                {
                                    Models = sinkRecord.Ready ? sinkRecord.Models.Count() : 0, sinkRecord.Status
                                });
                            sinkBatch.Add(sinkRecord);
                        }
                    },
                    (record, exception) => exception.SetLogContext(record),
                    parallelism);
                }
            }

            if (sinkBatch.Any(b => b.Ready))
            {
                await Put(connector, taskId, sinkBatch);
            }
            sinkBatch.ForEach(record => record.UpdateStatus());
            return batches;
        }
    }

    public Task Startup(string connector) => Task.CompletedTask;

    protected abstract Task Put(string connector, int taskId,  BlockingCollection<SinkRecord<TModel>> sinkBatch);

    public Task Cleanup(string connector) => Task.CompletedTask;

    public bool IsOfType(string connector, string plugin, string handler) =>
        plugin == _configurationProvider.GetPluginName(connector) && GetType().FullName == handler;
}