using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Handlers
{
    public class RetriableHandler : IRetriableHandler
    {
        private readonly ILogger<RetriableHandler> _logger;
        private readonly ISinkExceptionHandler _sinkExceptionHandler;
        private readonly IConfigurationProvider _configurationProvider;

        public RetriableHandler(ILogger<RetriableHandler> logger, ISinkExceptionHandler sinkExceptionHandler, IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _sinkExceptionHandler = sinkExceptionHandler;
            _configurationProvider = configurationProvider;
        }

        public async Task<SinkRecordBatch> Retry(Func<Task<SinkRecordBatch>> consumer, string connector)
        {
            var retryConfig = _configurationProvider.GetRetriesConfig(connector) ?? new RetryConfig();
            var (_, _, consumedBatch) =
                await RetryInternal(consumer, attempts:retryConfig.Attempts, delayTimeoutMs:retryConfig.DelayTimeoutMs);
            return consumedBatch;
        }


        public async Task<SinkRecordBatch> Retry(Func<SinkRecordBatch, Task<SinkRecordBatch>> handler, SinkRecordBatch batch, string connector)
        {
            batch.Started();
            var retryConfig = _configurationProvider.GetRetriesConfig(connector) ?? new RetryConfig();
            var (remaining, splitBatch, sinkRecordBatch) =
                await RetryInternal(() => handler(batch),  batch, retryConfig.Attempts, retryConfig.DelayTimeoutMs);
            
            batch = sinkRecordBatch ?? batch;

            if (remaining < 0 || !splitBatch)
            {
                batch.Completed();
                return batch;
            }

            --remaining;
            _logger.LogDebug("{@Log}", new {Message = "The batch will be split, and messages will be processed individually"});
            batch.IsLastAttempt = true;
            foreach (var record in batch) 
            {
                var singleBatch = new SinkRecordBatch(connector, record) {IsLastAttempt = true};
                await RetryInternal(() => handler(singleBatch), singleBatch, remaining, retryConfig.DelayTimeoutMs, true);
                singleBatch.Completed();
            }

            return batch;
        }

        private async Task<(bool retry, bool split)> IsRetrying(int attempts, int delayTimeoutMs, ConnectException ex,
            bool throwOnLastAttempt)
        {
            _sinkExceptionHandler.LogRetryException(ex, attempts);
            await Task.Delay(delayTimeoutMs);
            return (attempts > 0, attempts > 0 && !throwOnLastAttempt && attempts == 1);
        }

        private async Task<(int remaining, bool split, SinkRecordBatch batch)> RetryInternal(Func<Task<SinkRecordBatch>> handler, SinkRecordBatch batch = null, int attempts = 3, int delayTimeoutMs = 1000, bool throwOnLastAttempt = false)
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
                            else if (cae.CanRetry || cae.GetAllExceptions().Count() < batch?.Count)
                            {
                                _sinkExceptionHandler.LogRetryException(cae, attempts);
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

        private static ConnectToleranceExceededException ThrowToleranceExceededException(params Exception[] exceptions)
        {
            return new ConnectToleranceExceededException(ErrorCode.Local_Fatal, exceptions);
        }
    }
}

