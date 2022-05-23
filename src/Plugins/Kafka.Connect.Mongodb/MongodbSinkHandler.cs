﻿using System;
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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Mongodb
{
    public class MongodbSinkHandler : ISinkHandler
    {
        private readonly ILogger<MongodbSinkHandler> _logger;
        private readonly IEnumerable<IWriteModelStrategyProvider> _writeModelStrategyProviders;
        private readonly IMongoSinkConfigProvider _mongoSinkConfigProvider;
        private readonly IMongoWriter _mongoWriter;
        private readonly string _plugin;

        public MongodbSinkHandler(ILogger<MongodbSinkHandler> logger,
            IEnumerable<IWriteModelStrategyProvider> writeModelStrategyProviders, IMongoSinkConfigProvider mongoSinkConfigProvider,
            IMongoWriter mongoWriter, string plugin = "")
        {
            _logger = logger;
            _writeModelStrategyProviders = writeModelStrategyProviders;
            _mongoSinkConfigProvider = mongoSinkConfigProvider;
            _mongoWriter = mongoWriter;
            _plugin = plugin;
        }

        [OperationLog("Invoking put.")]
        public async Task<SinkRecordBatch> Put(SinkRecordBatch batches)
        {
            _logger.LogTrace("Building write models...");
            var mongoSinkConfig = _mongoSinkConfigProvider.GetMongoSinkConfig(batches.Connector);

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
                                _logger.LogDebug("{@debug}", new {message = "Record already saved to mongodb."});
                                return;
                            }

                            var sinkRecord = MongoSinkRecord.Create(record);
                            if (!record.Skip)
                            {
                                var strategy =
                                    _writeModelStrategyProviders.GetWriteModelStrategy(mongoSinkConfig.Properties
                                        .WriteStrategy, sinkRecord);
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
                                _logger.LogTrace("{@debug}",
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
                            _logger.LogTrace("{@debug}",
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
                        mongoSinkConfig.Batch?.Parallelism ?? 100);
                }
            }

            if (toWrite.Any(s => s.ReadyToWrite))
            {
                await _mongoWriter.WriteMany(
                    toWrite.Where(s => s.ReadyToWrite)
                        .OrderBy(s => s.Topic)
                        .ThenBy(s => s.Partition)
                        .ThenBy(s => s.Offset).ToList(),
                    mongoSinkConfig); //lets preserve the order
            }

            foreach (var mongoSinkRecord in toWrite)
            {
                mongoSinkRecord.CanCommitOffset = mongoSinkRecord.Skip || mongoSinkRecord.ReadyToWrite;
                mongoSinkRecord.UpdateStatus();
            }

            return batches;
        }
        
        [OperationLog("Running mongodb sink handler startup script.")]
        public async Task Startup(string connector)
        {
            await _mongoWriter.CreateCollection(_mongoSinkConfigProvider.GetMongoSinkConfig(connector));
        }
        
        [OperationLog("Running mongodb sink handler cleanup script.")]
        public Task Cleanup(string connector)
        {
            _logger?.LogTrace("From Mongo sink handler initializer - lets do some cleanup tasks...");
            return Task.CompletedTask;
        }

        public bool IsOfType(string plugin, string type)
        {
            return _plugin == plugin && GetType().FullName == type;
        }
    }
}