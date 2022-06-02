using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Utilities;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Handlers
{
    public class SinkExceptionHandler : ISinkExceptionHandler
    {
        private readonly ILogger<SinkExceptionHandler> _logger;
        private readonly IConnectDeadLetter _connectDeadLetter;
        private readonly IConfigurationProvider _configurationProvider;

        public SinkExceptionHandler(ILogger<SinkExceptionHandler> logger, IConnectDeadLetter connectDeadLetter, IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _connectDeadLetter = connectDeadLetter;
            _configurationProvider = configurationProvider;
        }

        [OperationLog("Handle processing errors.")]
        public void Handle(Exception exception, Action cancel)
        {
            switch (exception)
            {
                case ConnectToleranceExceededException tee:
                    foreach (var ce in tee.GetConnectExceptions())
                    {
                        using (LogContext.Push(new PropertyEnricher("Topic", ce.Topic),
                            new PropertyEnricher("Partition", ce.Partition), new PropertyEnricher("Offset", ce.Offset)))
                        {
                            _logger.LogError(ce.InnerException, "{@Log}",
                                new {Status = SinkStatus.Failed, Message = "Tolerance exceeded in error handler"});
                        }
                    }

                    foreach (var ex in tee.GetNonConnectExceptions())
                    {
                        _logger.LogError(ex, "{@Log}",
                            new {Status = SinkStatus.Failed, Message = "Tolerance exceeded in error handler"});
                    }

                    cancel();
                    break;
                case ConnectAggregateException cae:
                    foreach (var ce in cae.GetConnectExceptions())
                    {
                        using (LogContext.Push(new PropertyEnricher("Topic", ce.Topic),
                            new PropertyEnricher("Partition", ce.Partition), new PropertyEnricher("Offset", ce.Offset)))
                        {
                            _logger.LogError(ce.InnerException, "{@Log}",
                                new {Status = SinkStatus.Failed, Message = "Tolerance exceeded in error handler."});
                        }
                    }

                    foreach (var ex in cae.GetNonConnectExceptions())
                    {
                        _logger.LogError(ex, "{@Log}",
                            new {Status = SinkStatus.Failed, Message = "Tolerance exceeded in error handler"});
                    }

                    cancel();
                    break;
                case ConnectDataException cde:
                    if (cde.InnerException is OperationCanceledException oce)
                    {
                        _logger.LogOperationCancelled(oce);
                    }
                    else
                    {
                        _logger.LogError(cde.InnerException, "{@Log}",
                            new {Status = SinkStatus.Failed, Message = "Unknown error detected. Task will be shutdown."});
                        cancel();
                    }

                    break;
                default:
                    _logger.LogError(exception, "{@Log}",
                        new {Status = SinkStatus.Failed, Message = "Unknown error detected. Task will be shutdown."});
                    cancel();
                    break;
            }
        }

        [OperationLog("Handle dead-letter.")]
        public async Task HandleDeadLetter(IEnumerable<SinkRecord> sinkRecords, Exception exception, string connector)
        {
            if (_configurationProvider.IsDeadLetterEnabled(connector))
            {
                await _connectDeadLetter.Send(sinkRecords.Where(r => r.Status == SinkStatus.Failed), exception, connector);
            }
        }

        public void LogRetryException(ConnectException connectException, int attempts)
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
    }
}