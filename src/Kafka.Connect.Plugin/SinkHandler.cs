using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

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

    public async Task<ConnectRecordModel> BuildModels(ConnectRecord record, string connector)
    {
        var sinkRecord = new ConnectRecord<TModel>(record);
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
        
        return sinkRecord;
    }

    public Task Put(BlockingCollection<ConnectRecordModel> models, string connector, int taskId) =>
        Put(models.Select(m => m as ConnectRecord<TModel>), connector, taskId);

    protected abstract Task Put(IEnumerable<ConnectRecord<TModel>> models, string connector, int taskId);
    
    public Task Startup(string connector) => Task.CompletedTask;

    public Task Cleanup(string connector) => Task.CompletedTask;

    public bool Is(string connector, string plugin, string handler) =>
        plugin == _configurationProvider.GetPluginName(connector) && this.Is(handler);
}