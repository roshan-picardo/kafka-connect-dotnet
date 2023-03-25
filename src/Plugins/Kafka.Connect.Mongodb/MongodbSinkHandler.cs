using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Mongodb.Collections;
using Kafka.Connect.Mongodb.Extensions;
using Kafka.Connect.Mongodb.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Mongodb
{
    public class MongodbSinkHandler : ISinkHandler
    {
        private readonly ILogger<MongodbSinkHandler> _logger;
        private readonly IEnumerable<IWriteModelStrategyProvider> _writeModelStrategyProviders;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IMongoWriter _mongoWriter;
        private readonly string _plugin;

        public MongodbSinkHandler(ILogger<MongodbSinkHandler> logger,
            IEnumerable<IWriteModelStrategyProvider> writeModelStrategyProviders, IConfigurationProvider configurationProvider,
            IMongoWriter mongoWriter, string plugin = "")
        {
            _logger = logger;
            _writeModelStrategyProviders = writeModelStrategyProviders;
            _configurationProvider = configurationProvider;
            _mongoWriter = mongoWriter;
            _plugin = plugin;
        }

        [OperationLog("Invoking put.")]
        public async Task<SinkRecordBatch> Put(SinkRecordBatch batches, string connector = null, int parallelism = 100)
        {
            connector ??= batches.Connector;
            var mongoSinkConfig = _configurationProvider.GetSinkConfigProperties<MongoSinkConfig>(connector);
            

            var toWrite = new BlockingCollection<MongoSinkRecord>();
            foreach (var batch in batches.BatchByTopicPartition)
            {
                using (LogContext.Push(new PropertyEnricher("topic", batch.Topic),
                    new PropertyEnricher("partition", batch.Partition)))
                {
                    await batch.Batch.ForEachAsync(async record =>
                        {
                            if (record.IsSaved)
                            {
                                _logger.LogDebug("{@Log}", new {message = "Record already saved to mongodb."});
                                return;
                            }

                            var sinkRecord = MongoSinkRecord.Create(record);
                            if (!record.Skip)
                            {
                                var strategy =
                                    _writeModelStrategyProviders.GetWriteModelStrategy(mongoSinkConfig.WriteStrategy, sinkRecord);
                                if (strategy == null)
                                {
                                    sinkRecord.Status = SinkStatus.Failed;
                                    // lets throw retriable as some of the messages might pass on last attempt
                                    throw new ConnectRetriableException(ErrorCode.Local_Application,
                                            new NullReferenceException(
                                                "Failed to load the Write Model Strategy. Check if the strategy is registered and configured for this record."))
                                        .SetLogContext(record);
                                }
                                (sinkRecord.Status, sinkRecord.WriteModels) = await strategy.CreateWriteModels(record);
                                _logger.LogTrace("{@Log}",
                                    new
                                    {
                                        message =
                                            $"Write Models created successfully: Strategy: {strategy.GetType().FullName}"
                                    });
                            }
                            else
                            {
                                sinkRecord.Status = SinkStatus.Skipping;
                            }
                            _logger.LogTrace("{@Log}",
                                new
                                {
                                    models = sinkRecord.ReadyToWrite ? sinkRecord.WriteModels.Count() : 0,
                                    status = sinkRecord.Status,
                                    message = sinkRecord.ReadyToWrite
                                        ? "Write models created successfully."
                                        : "Sink record marked for skipping."
                                });
                            toWrite.Add(sinkRecord);
                        },
                        (record, exception) => exception.SetLogContext(record),
                        parallelism);
                }
            }

            if (toWrite.Any(s => s.ReadyToWrite))
            {
                await _mongoWriter.WriteMany(
                    toWrite.Where(s => s.ReadyToWrite)
                        .OrderBy(s => s.Topic)
                        .ThenBy(s => s.Partition)
                        .ThenBy(s => s.Offset).ToList(),
                    mongoSinkConfig, connector); //lets preserve the order
            }

            foreach (var mongoSinkRecord in toWrite)
            {
                mongoSinkRecord.CanCommitOffset = mongoSinkRecord.Skip || mongoSinkRecord.ReadyToWrite;
                mongoSinkRecord.UpdateStatus();
            }

            return batches;
        }

        public Task Startup(string connector)
        {
            return Task.CompletedTask;
        }

        public Task Cleanup(string connector)
        {
            return Task.CompletedTask;
        }

        public bool IsOfType(string plugin, string type)
        {
            return _plugin == plugin && GetType().FullName == type;
        }
    }
}