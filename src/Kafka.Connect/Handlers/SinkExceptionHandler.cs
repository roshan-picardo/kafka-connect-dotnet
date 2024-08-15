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

namespace Kafka.Connect.Handlers;

public class SinkExceptionHandler(
    ILogger<SinkExceptionHandler> logger,
    IConnectDeadLetter connectDeadLetter,
    IConfigurationProvider configurationProvider)
    : ISinkExceptionHandler
{
    public void Handle(Exception exception, Action cancel)
    {
        using (logger.Track("Handle processing errors."))
        {
            const string logToleranceExceeded = "Tolerance exceeded in error handler.";
            const string logUnknownError = "Unknown error detected. Task will be shutdown.";
            switch (exception)
            {
                case ConnectToleranceExceededException tee:
                    foreach (var ce in tee.GetConnectExceptions())
                    {
                        using (ConnectLog.TopicPartitionOffset(ce.Topic, ce.Partition, ce.Offset))
                        {
                            logger.Error(logToleranceExceeded, new { Status = Status.Failed },
                                ce.InnerException);
                        }
                    }

                    foreach (var ex in tee.GetNonConnectExceptions())
                    {
                        logger.Error(logToleranceExceeded, new { Status = Status.Failed }, ex);
                    }

                    cancel();
                    break;
                case ConnectAggregateException cae:
                    foreach (var ce in cae.GetConnectExceptions())
                    {
                        using (ConnectLog.TopicPartitionOffset(ce.Topic, ce.Partition, ce.Offset))
                        {
                            logger.Error(logToleranceExceeded, new { Status = Status.Failed },
                                ce.InnerException);
                        }
                    }

                    foreach (var ex in cae.GetNonConnectExceptions())
                    {
                        logger.Error(logToleranceExceeded, new { Status = Status.Failed }, ex);
                    }

                    cancel();
                    break;
                case ConnectDataException cde:
                    if (cde.InnerException is OperationCanceledException oce)
                    {
                        if (oce.CancellationToken.IsCancellationRequested)
                        {
                            logger.Info("Worker shutdown initiated. Connector task will be shutdown.",
                                new { Status = Status.Failed });
                        }
                        else
                        {
                            logger.Error("Unexpected error while shutting down the Worker.",
                                new { Status = Status.Failed }, oce);
                        }
                    }
                    else
                    {
                        logger.Error(logUnknownError, new { Status = Status.Failed }, cde.InnerException);
                        cancel();
                    }

                    break;
                default:
                    logger.Error(logUnknownError, new { Status = Status.Failed }, exception);
                    cancel();
                    break;
            }
        }
    }
        
    public void Handle2(Exception exception, Action cancel)
    {
        using (logger.Track("Handle processing errors."))
        {
            switch (exception)
            {
                case ConnectToleranceExceededException:
                    cancel();
                    break;
                case ConnectAggregateException:
                    cancel();
                    break;
                case ConnectDataException cde:
                    if (cde.InnerException is not OperationCanceledException)
                    {
                        cancel();
                    }
                    break;
                default:
                    cancel();
                    break;
            }
        }
    }

    public async Task HandleDeadLetter(IList<SinkRecord> batch, Exception exception, string connector)
    {
        using (logger.Track("Handle dead-letter."))
        {
            if (configurationProvider.IsDeadLetterEnabled(connector))
            {
                await connectDeadLetter.Send(batch.Where(r => r.Status == Status.Failed), exception,
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
                    using (ConnectLog.TopicPartitionOffset(ce.Topic, ce.Partition, ce.Offset))
                    {
                        logger.Error(message, new { Status = Status.Failed }, ce);
                    }
                }

                foreach (var oe in cae.GetNonConnectExceptions())
                {
                    logger.Error(message, new { Status = Status.Failed }, oe);
                }

                break;
            default:
                using (ConnectLog.TopicPartitionOffset(connectException.Topic, connectException.Partition, connectException.Offset))
                {
                    logger.Error(message, new { Status = Status.Failed }, connectException);
                }

                break;
        }
    }
}
