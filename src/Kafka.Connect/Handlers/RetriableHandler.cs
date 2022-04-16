using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using Serilog.Core.Enrichers;
using Kafka.Connect.Configurations;

namespace Kafka.Connect.Handlers
{
    public class RetriableHandler : IRetriableHandler
    {
        private readonly ILogger<RetriableHandler> _logger;
        private readonly IConfigurationProvider _configurationProvider;

        public RetriableHandler(ILogger<RetriableHandler> logger, IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _configurationProvider = configurationProvider;
        }

        public async Task<SinkRecordBatch> Retry(Func<Task<SinkRecordBatch>> consumer, int attempts, int delayTimeoutMs)
        {
            var (_, _, consumedBatch) =
                await RetryInternal(consumer, attempts: attempts, delayTimeoutMs: delayTimeoutMs);
            return consumedBatch;
        }


        public async Task Retry(Func<SinkRecordBatch, Task<Guid>> handler, SinkRecordBatch consumedBatch, int attempts,
            int delayTimeoutMs)
        {
            var (remaining, splitBatch, _) =
                await RetryInternal(async () => await handler(consumedBatch), attempts, delayTimeoutMs);

            if (remaining >= 0 && splitBatch) // else the whole batch is processed successfully
            {
                --remaining;
                _logger.LogDebug("{@Log}", new {Message = "The batch will be split, and messages will be processed individually"});
                foreach (var record in consumedBatch)
                {
                    var singleBatch = consumedBatch.Single(record);
                    await RetryInternal(async () => await handler(singleBatch), remaining, delayTimeoutMs, true);
                }
            }
        }

        public async Task<SinkRecordBatch> Retry(Func<SinkRecordBatch, Task<SinkRecordBatch>> handler, SinkRecordBatch batch, string connector)
        {
            var retryConfig = _configurationProvider.GetRetriesConfig(connector);
            var (remaining, splitBatch, sinkRecordBatch) =
                await RetryInternal(() => handler(batch),  retryConfig.Attempts, retryConfig.DelayTimeoutMs);
            
            batch = sinkRecordBatch ?? batch;
            
            if (remaining < 0 || !splitBatch) return batch;
            --remaining;
            _logger.LogDebug("{@Log}", new {Message = "The batch will be split, and messages will be processed individually"});
            batch.IsLastAttempt = true;
            foreach (var record in batch)
            {
                var singleBatch = new SinkRecordBatch(connector, record) {IsLastAttempt = true};
                await RetryInternal(() => handler(singleBatch), remaining, retryConfig.DelayTimeoutMs, true);
            }

            return batch;
        }

        public async Task<SinkRecordBatch> Retry(Func<SinkRecordBatch, Task<SinkRecordBatch>> handler, SinkRecordBatch consumedBatch, int attempts,
            int delayTimeoutMs)
        {
            var (remaining, splitBatch, sinkRecordBatch) =
                await RetryInternal(() => handler(consumedBatch), attempts, delayTimeoutMs);
            sinkRecordBatch ??= consumedBatch;
            
            if (remaining < 0 || !splitBatch) return sinkRecordBatch;
            --remaining;
            _logger.LogDebug("{@Log}", new {Message = "The batch will be split, and messages will be processed individually"});
            foreach (var record in consumedBatch)
            {
                var singleBatch = consumedBatch.Single(record);
                singleBatch.IsLastAttempt = true;
                await RetryInternal(() => handler(singleBatch), remaining, delayTimeoutMs, true);
            }

            return sinkRecordBatch;
        }

        private void LogException(ConnectException connectException, int attempts)
        {
            var message = $"Message processing failed. Remaining retries: {attempts}";
            switch (connectException)
            {
                case ConnectAggregateException cae:
                    foreach (var ce in cae.GetConnectExceptions())
                    {
                        using (LogContext.Push(new PropertyEnricher("Topic", ce.Topic),
                            new PropertyEnricher("Partition", ce.Partition), new PropertyEnricher("Offset", ce.Offset)))
                        {
                            _logger.LogError(ce,"{@Log}", new { Status = SinkStatus.Failed, Message = message});
                        }
                    }

                    foreach (var oe in cae.GetNonConnectExceptions())
                    {
                        _logger.LogError(oe, "{@Log}", new { Status = SinkStatus.Failed, Message = message});
                    }

                    break;
                default:
                    using (LogContext.Push(new PropertyEnricher("Topic", connectException.Topic),
                        new PropertyEnricher("Partition", connectException.Partition),
                        new PropertyEnricher("Offset", connectException.Offset)))
                    {
                        _logger.LogError(connectException, "{@Log}", new { Status = SinkStatus.Failed, Message = message});
                    }

                    break;
            }
        }
        
        
        private async Task<(bool, bool)> IsRetrying(int attempts, int delayTimeoutMs,
            ConnectException ex, bool throwOnLastAttempt)
        {
            using (LogContext.Push(new PropertyEnricher("Topic", ex.Topic),
                new PropertyEnricher("Partition", ex.Partition), new PropertyEnricher("Offset", ex.Offset)))
            {
                if (attempts > 1) 
                {
                    LogException(ex, attempts);
                    await Task.Delay(delayTimeoutMs);
                    return (true, false);
                }

                if (attempts != 1) return (false, false);
                
                LogException(ex, attempts);
                await Task.Delay(delayTimeoutMs);
                return throwOnLastAttempt ? (false, false): (true, true);
            }
        }
        
        private async Task<(int, bool, T)> RetryInternal<T>(Func<Task<T>> handler, int attempts = 3, int delayTimeoutMs = 1000, bool throwOnLastAttempt = false)
        {
            do
            {
                try
                {
                    return (-1, false,  await handler());
                }
                catch (Exception ex)
                {
                    switch (ex)
                    {
                        case ConnectAggregateException cae:
                            if (cae.ShouldRetry)
                            {
                                var (aRetry, aSplit) = await IsRetrying(attempts, delayTimeoutMs, cae, throwOnLastAttempt);
                                if (!aRetry)
                                {
                                    throw ThrowToleranceExceededException(cae.GetAllExceptions().ToArray());
                                }

                                if (aSplit)
                                {
                                    return (attempts, true, default);
                                }
                            }
                            else if (cae.CanRetry)
                            {
                                LogException(cae, attempts);
                                return (attempts, true, default);
                            }
                            else throw ThrowToleranceExceededException(cae.GetAllExceptions().ToArray());

                            continue;
                        case ConnectRetriableException cre:
                            var (rRetry, _) = await IsRetrying(attempts, delayTimeoutMs, cre, throwOnLastAttempt);
                            if (!rRetry)
                            {
                                throw ThrowToleranceExceededException(cre.InnerException);
                            }
                            
                            continue;
                        case ConnectDataException cde:
                            throw ThrowToleranceExceededException(cde.InnerException);
                        default:
                            throw ThrowToleranceExceededException(ex);
                    }

                }
            } while (--attempts >= 0);

            return (-1, false, default);
        }

        private static ConnectToleranceExceededException ThrowToleranceExceededException(SinkRecordBatch batch,
            params Exception[] exceptions)
        {
            var failedBatch = batch.Select(r =>
            {
                r.Status = SinkStatus.Failed;
                return r;
            });
            return new ConnectToleranceExceededException(ErrorCode.Local_Fatal, failedBatch, exceptions );
        }
        
        private static ConnectToleranceExceededException ThrowToleranceExceededException(params Exception[] exceptions)
        {
            return new ConnectToleranceExceededException(ErrorCode.Local_Fatal, exceptions );
        }
    }
}

