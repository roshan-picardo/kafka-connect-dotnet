using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Handlers
{
    public class SinkExceptionHandler : ISinkExceptionHandler
    {
        private readonly ILogger<SinkExceptionHandler> _logger;
        private readonly IConnectDeadLetter _connectDeadLetter;
        private readonly IConfigurationProvider _configurationProvider;

        public SinkExceptionHandler(
            ILogger<SinkExceptionHandler> logger,
            IConnectDeadLetter connectDeadLetter,
            IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _connectDeadLetter = connectDeadLetter;
            _configurationProvider = configurationProvider;
        }

        public void Handle(Exception exception, Action cancel)
        {
            using (_logger.Track("Handle processing errors."))
            {
                const string logToleranceExceeded = "Tolerance exceeded in error handler.";
                const string logUnknownError = "Unknown error detected. Task will be shutdown.";
                switch (exception)
                {
                    case ConnectToleranceExceededException tee:
                        foreach (var ce in tee.GetConnectExceptions())
                        {
                            using (LogContext.Push(new PropertyEnricher("Topic", ce.Topic),
                                       new PropertyEnricher("Partition", ce.Partition),
                                       new PropertyEnricher("Offset", ce.Offset)))
                            {
                                _logger.Error(logToleranceExceeded, new { Status = SinkStatus.Failed },
                                    ce.InnerException);
                            }
                        }

                        foreach (var ex in tee.GetNonConnectExceptions())
                        {
                            _logger.Error(logToleranceExceeded, new { Status = SinkStatus.Failed }, ex);
                        }

                        cancel();
                        break;
                    case ConnectAggregateException cae:
                        foreach (var ce in cae.GetConnectExceptions())
                        {
                            using (LogContext.Push(new PropertyEnricher("Topic", ce.Topic),
                                       new PropertyEnricher("Partition", ce.Partition),
                                       new PropertyEnricher("Offset", ce.Offset)))
                            {
                                _logger.Error(logToleranceExceeded, new { Status = SinkStatus.Failed },
                                    ce.InnerException);
                            }
                        }

                        foreach (var ex in cae.GetNonConnectExceptions())
                        {
                            _logger.Error(logToleranceExceeded, new { Status = SinkStatus.Failed }, ex);
                        }

                        cancel();
                        break;
                    case ConnectDataException cde:
                        if (cde.InnerException is OperationCanceledException oce)
                        {
                            if (oce.CancellationToken.IsCancellationRequested)
                            {
                                _logger.Info("Worker shutdown initiated. Connector task will be shutdown.",
                                    new { Status = SinkStatus.Failed });
                            }
                            else
                            {
                                _logger.Error("Unexpected error while shutting down the Worker.",
                                    new { Status = SinkStatus.Failed }, oce);
                            }
                        }
                        else
                        {
                            _logger.Error(logUnknownError, new { Status = SinkStatus.Failed }, cde.InnerException);
                            cancel();
                        }

                        break;
                    default:
                        _logger.Error(logUnknownError, new { Status = SinkStatus.Failed }, exception);
                        cancel();
                        break;
                }
            }
        }

        public async Task HandleDeadLetter(ConnectRecordBatch batch, Exception exception, string connector)
        {
            using (_logger.Track("Handle dead-letter."))
            {
                if (_configurationProvider.IsDeadLetterEnabled(connector))
                {
                    await _connectDeadLetter.Send(batch.GetAll<Models.SinkRecord>().Where(r => r.Status == SinkStatus.Failed), exception,
                        connector);
                }
            }
        }

        public async Task HandleDeadLetter(IList<SinkRecord> batch, Exception exception, string connector)
        {
            using (_logger.Track("Handle dead-letter."))
            {
                if (_configurationProvider.IsDeadLetterEnabled(connector))
                {
                    await _connectDeadLetter.Send(batch.Where(r => r.Status == SinkStatus.Failed), exception,
                        connector);
                }
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
                            _logger.Error(message, new { Status = SinkStatus.Failed }, ce);
                        }
                    }

                    foreach (var oe in cae.GetNonConnectExceptions())
                    {
                        _logger.Error(message, new { Status = SinkStatus.Failed }, oe);
                    }

                    break;
                default:
                    using (LogContext.Push(new PropertyEnricher("Topic", connectException.Topic),
                        new PropertyEnricher("Partition", connectException.Partition),
                        new PropertyEnricher("Offset", connectException.Offset)))
                    {
                        _logger.Error(message, new { Status = SinkStatus.Failed }, connectException);
                    }

                    break;
            }
        }
    }
}