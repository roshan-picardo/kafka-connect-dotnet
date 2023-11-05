using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Connectors;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Handlers
{
    public class SinkExceptionHandlerTests
    {
        private readonly global::Kafka.Connect.Plugin.Logging.ILogger<SinkExceptionHandler> _logger;
        private readonly IConnectDeadLetter _connectDeadLetter;
        private readonly IConfigurationProvider _configurationProvider;

        private readonly SinkExceptionHandler _sinkExceptionHandler;

        public SinkExceptionHandlerTests()
        {
            _logger = Substitute.For<global::Kafka.Connect.Plugin.Logging.ILogger<SinkExceptionHandler>>();
            _connectDeadLetter = Substitute.For<IConnectDeadLetter>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();

            _sinkExceptionHandler = new SinkExceptionHandler(_logger, _connectDeadLetter, _configurationProvider);
        }

        [Theory]
        [InlineData(new [] { "retriable-exception", "data-exception", "any-exception" }, 3)]
        [InlineData(new [] { "retriable-exception" }, 1)]
        [InlineData(new [] { "any-exception" }, 1)]
        [InlineData(new [] { "any-exception", "any-exception" }, 2)]
        [InlineData(null, 0)]
        public void Handle_LogConnectToleranceExceededException(string[] exceptions, int expected)
        {
            var innerExceptions = new List<Exception>();
            foreach (var exception in exceptions ?? Array.Empty<string>())
            {
                switch (exception)
                {
                    case "retriable-exception": 
                        innerExceptions.Add(new ConnectRetriableException(ErrorCode.Unknown.GetReason(), new Exception())); 
                        break;
                    case "data-exception": 
                        innerExceptions.Add(new ConnectDataException(ErrorCode.Unknown.GetReason(), new Exception())); 
                        break;
                    case "any-exception": 
                        innerExceptions.Add(new Exception()); 
                        break;
                }
            }

            var connectToleranceExceededException = new ConnectToleranceExceededException(ErrorCode.Unknown.GetReason(), innerExceptions.ToArray());
            var token = new CancellationTokenSource();
            _sinkExceptionHandler.Handle(connectToleranceExceededException, () => { token.Cancel(); });
            
            _logger.Received(expected).Error("Tolerance exceeded in error handler.", Arg.Any<object>(), Arg.Any<Exception>());
            Assert.True(token.IsCancellationRequested);
        }
        
        [Theory]
        [InlineData(new [] { "retriable-exception", "data-exception", "any-exception" }, 3)]
        [InlineData(new [] { "retriable-exception" }, 1)]
        [InlineData(new [] { "any-exception" }, 1)]
        [InlineData(new [] { "any-exception", "any-exception" }, 2)]
        [InlineData(null, 0)]
        public void Handle_LogConnectAggregateException(string[] exceptions, int expected)
        {
            var innerExceptions = new List<Exception>();
            foreach (var exception in exceptions ?? Array.Empty<string>())
            {
                switch (exception)
                {
                    case "retriable-exception": 
                        innerExceptions.Add(new ConnectRetriableException(ErrorCode.Unknown.GetReason(), new Exception())); 
                        break;
                    case "data-exception": 
                        innerExceptions.Add(new ConnectDataException(ErrorCode.Unknown.GetReason(), new Exception())); 
                        break;
                    case "any-exception": 
                        innerExceptions.Add(new Exception()); 
                        break;
                }
            }

            var connectToleranceExceededException = new ConnectAggregateException(ErrorCode.Unknown.GetReason(), innerExceptions:innerExceptions.ToArray());
            var token = new CancellationTokenSource();
            _sinkExceptionHandler.Handle(connectToleranceExceededException, () => { token.Cancel(); });
            
            _logger.Received(expected).Error("Tolerance exceeded in error handler.", Arg.Any<object>(), Arg.Any<Exception>());
            Assert.True(token.IsCancellationRequested);
        }
        
        [Theory]
        [InlineData("operation-cancelled-exception", false, LogLevel.Error, "Unexpected error while shutting down the Worker.")]
        [InlineData("operation-cancelled-exception", true, LogLevel.Information, "Worker shutdown initiated. Connector task will be shutdown.")]
        [InlineData("any-exception", false, LogLevel.Error, "Unknown error detected. Task will be shutdown.")]
        public void Handle_LogConnectDataException(string inner, bool cancellationRequested, LogLevel level, string logMessage)
        {
            Exception innerException = null;
            switch (inner)
            {
                case "operation-cancelled-exception":
                    var ocToken = new CancellationTokenSource();
                    if (cancellationRequested)
                    {
                        ocToken.Cancel();
                    }
                    innerException = new OperationCanceledException(ocToken.Token);
                    break;
                case "any-exception":
                    innerException = new Exception();
                    break;
            }

            var connectDataException = new ConnectDataException(ErrorCode.Unknown.GetReason(), innerException);
            var token = new CancellationTokenSource();
            _sinkExceptionHandler.Handle(connectDataException, () => { token.Cancel(); });

            _logger.Received(level == LogLevel.Information ? 0 : 1).Error(logMessage, Arg.Any<object>(), Arg.Any<Exception>());
            _logger.Received(level == LogLevel.Error ? 0 : 1).Info(logMessage, Arg.Any<object>(), Arg.Any<Exception>());

            Assert.Equal(innerException  is not OperationCanceledException,  token.IsCancellationRequested);
        }

        [Fact]
        public void Handle_LogAnyException()
        {
            var token = new CancellationTokenSource();
            _sinkExceptionHandler.Handle(new Exception(), () => { token.Cancel(); });

            _logger.Received().Error("Unknown error detected. Task will be shutdown.",Arg.Any<object>(), Arg.Any<Exception>());

            Assert.True(token.IsCancellationRequested);
        }

        [Theory]
        [InlineData(false, 0, 0)]
        [InlineData(true, 0, 1)]
        [InlineData(true, 1, 1)]
        [InlineData(true, 2, 1)]
        public async Task HandleDeadLetter_SendToQueue(bool isEnabled, int failedCount, int expected)
        {
            _configurationProvider.IsDeadLetterEnabled(Arg.Any<string>()).Returns(isEnabled);
            var batch = GetBatch(2, failedCount);
            var exception = new Exception();
            
            await _sinkExceptionHandler.HandleDeadLetter(batch, exception, "connector");

            await _connectDeadLetter.Received(expected).Send(Arg.Is<IEnumerable<global::Kafka.Connect.Models.SinkRecord>>(s => s.Count() == failedCount),
                exception, "connector");
        }
        
        
        [Theory]
        [InlineData(new [] { "retriable-exception", "data-exception", "any-exception" }, 3)]
        [InlineData(new [] { "retriable-exception" }, 1)]
        [InlineData(new [] { "any-exception" }, 1)]
        [InlineData(new [] { "any-exception", "any-exception" }, 2)]
        [InlineData(null, 0)]
        public void LogRetryException_LogConnectAggregateException(string[] exceptions, int expected)
        {
            var attempts = 3;
            var innerExceptions = new List<Exception>();
            foreach (var exception in exceptions ?? Array.Empty<string>())
            {
                switch (exception)
                {
                    case "retriable-exception": 
                        innerExceptions.Add(new ConnectRetriableException(ErrorCode.Unknown.GetReason(), new Exception())); 
                        break;
                    case "data-exception": 
                        innerExceptions.Add(new ConnectDataException(ErrorCode.Unknown.GetReason(), new Exception())); 
                        break;
                    case "any-exception": 
                        innerExceptions.Add(new Exception()); 
                        break;
                }
            }

            var connectToleranceExceededException = new ConnectAggregateException(ErrorCode.Unknown.GetReason(), innerExceptions:innerExceptions.ToArray());
            _sinkExceptionHandler.LogRetryException(connectToleranceExceededException, attempts);
            
            _logger.Received(expected).Error($"Message processing failed. Remaining retries: {attempts}", Arg.Any<object>(), Arg.Any<Exception>());
        }
        
        [Theory]
        [InlineData("data-exception", 2)]
        [InlineData("retriable-exception", 1)]
        [InlineData("connect-exception", 1)]
        public void LogRetryException_LogConnectException(string exception, int attempts)
        {
            var connectException = exception switch
            {
                "data-exception" => new ConnectDataException(ErrorCode.Unknown.GetReason(), new Exception()),
                "retriable-exception" => new ConnectRetriableException(ErrorCode.Unknown.GetReason(), new Exception()),
                _ => new ConnectException()
            };
            _sinkExceptionHandler.LogRetryException(connectException, attempts);

            _logger.Received().Error( $"Message processing failed. Remaining retries: {attempts}",Arg.Any<object>(), Arg.Any<Exception>());
        }
        
        private static ConnectRecordBatch GetBatch(int length = 2, int failed = 1)
        {
            var batch = new ConnectRecordBatch("connector");

            for (var i = 0; i < length; i++)
            {
                batch.Add(new global::Kafka.Connect.Models.SinkRecord(new ConsumeResult<byte[], byte[]>
                    {Topic = "topic", Message = new Message<byte[], byte[]>() {Headers = new Headers()}})
                {
                    Status = failed-- > 0 ? SinkStatus.Failed : SinkStatus.Updated
                });
            }

            return batch;
        }
    }
}