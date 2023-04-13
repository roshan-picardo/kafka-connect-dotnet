using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Handlers
{
    public class RetriableHandlerTests
    {
        private readonly ILogger<RetriableHandler> _logger;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly ISinkExceptionHandler _sinkExceptionHandler;
        private readonly IRetriableHandler _retriableHandler;

        public RetriableHandlerTests()
        {
            _logger = Substitute.For<ILogger<RetriableHandler>>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _sinkExceptionHandler = Substitute.For<ISinkExceptionHandler>();
            _retriableHandler = new RetriableHandler(_logger, _sinkExceptionHandler, _configurationProvider);
        }

        [Fact]
        public async Task Retry_ConsumeReturnsSinkRecordBatch()
        {
            _configurationProvider.GetRetriesConfig(Arg.Any<string>()).Returns(new RetryConfig {Attempts = 3, DelayTimeoutMs = 1});
            var sinkRecordBatch = new SinkRecordBatch("");
            Task<SinkRecordBatch> Consume() => Task.FromResult(sinkRecordBatch);

            var actual = await _retriableHandler.Retry(Consume, "connector");
            
            Assert.NotNull(actual);
            Assert.Equal(sinkRecordBatch, actual);
        }

        [Fact]
        public async Task Retry_ThrowsSingleConnectAggregateException()
        {
            _configurationProvider.GetRetriesConfig(Arg.Any<string>()).Returns(new RetryConfig {Attempts = 3, DelayTimeoutMs = 1});
            Task<SinkRecordBatch> Process() => throw new ConnectAggregateException(ErrorCode.Unknown, new Exception());

            await Assert.ThrowsAsync<ConnectToleranceExceededException>(() =>
                _retriableHandler.Retry(_ => Process(), GetBatch(), "connector"));
        }

        [Fact]
        public async Task Retry_ThrowsSingleConnectDataException()
        {
            _configurationProvider.GetRetriesConfig(Arg.Any<string>()).Returns(new RetryConfig {Attempts = 3, DelayTimeoutMs = 1});
            Task<SinkRecordBatch> Process() => throw new ConnectDataException(ErrorCode.Unknown, new Exception());
            
            await Assert.ThrowsAsync<ConnectToleranceExceededException>(() =>
                _retriableHandler.Retry(_ => Process(), GetBatch(), "connector"));
        }

        [Fact]
        public async Task Retry_ThrowsSingleGenericException()
        {
            _configurationProvider.GetRetriesConfig(Arg.Any<string>()).Returns(new RetryConfig {Attempts = 3, DelayTimeoutMs = 1});
            Task<SinkRecordBatch> Process() => throw new Exception();
            
            await Assert.ThrowsAsync<ConnectToleranceExceededException>(() =>
                _retriableHandler.Retry(_ => Process(), GetBatch(), "connector"));
        }
        
        [Fact]
        public async Task Retry_ThrowsSingleRetriableException()
        {
            _configurationProvider.GetRetriesConfig(Arg.Any<string>()).Returns(new RetryConfig {Attempts = 3, DelayTimeoutMs = 1});
            Task<SinkRecordBatch> Process() => throw new ConnectRetriableException(ErrorCode.Unknown, new Exception());
            
            await Assert.ThrowsAsync<ConnectToleranceExceededException>(() =>
                _retriableHandler.Retry(_ => Process(), GetBatch(), "connector"));
        }
        
        [Theory]
        [InlineData(2, new [] {"retriable-exception", "retriable-exception"}, 4, new[] { 3, 2, 1, 0})]
        [InlineData(2, new [] {"retriable-exception", "data-exception"}, 4, new[] { 3, 2, 1, 0})]
        [InlineData(2, new [] {"retriable-exception", "any-exception"}, 4, new[] { 3, 2, 1, 0})]
        [InlineData(2, new [] {"retriable-exception", "no-exception"}, 4, new[] { 3, 2, 1, 0})]
        [InlineData(2, new [] {"data-exception", "retriable-exception"}, 2, new[] {3} )]
        [InlineData(2, new [] {"data-exception", "data-exception"}, 1, null)]
        [InlineData(2, new [] {"data-exception", "any-exception"}, 1, null)]
        [InlineData(2, new [] {"data-exception", "no-exception"}, 2, new[] {3})] 
        [InlineData(2, new [] {"any-exception", "retriable-exception"}, 2, new[] {3} )]
        [InlineData(2, new [] {"any-exception", "data-exception"}, 1, null)]
        [InlineData(2, new [] {"any-exception", "any-exception"}, 1, null)]
        [InlineData(2, new [] {"any-exception", "no-exception"}, 2, new[] {3})] 
        [InlineData(2, new [] {"no-exception", "retriable-exception"}, 5, new[] {3, 2, 1, 0} )]
        [InlineData(2, new [] {"no-exception", "data-exception"}, 3, new[] {3})]
        [InlineData(2, new [] {"no-exception", "any-exception"}, 3, new[] {3})]
        //[InlineData(2, new [] {"no-exception", "no-exception"}, 1, null)] //Doesn't seem correct??
        public async Task Retry_ThrowsConnectAggregateException(int batchSize, string[] exceptions, int expectedCalls, int[] expectedLogAttempts)
        {
            var sinkRecordBatch = GetBatch(batchSize, exceptions);
            var callCounter = 0;
            Task<SinkRecordBatch> Process(SinkRecordBatch batch)
            {
                ++callCounter;
                var innerExceptions = new List<Exception>();
                foreach (var record in batch)
                {
                    switch (record.Topic)
                    {
                        case "retriable-exception":
                            innerExceptions.Add(new ConnectRetriableException(ErrorCode.Unknown, new Exception()));
                            break;
                        case "data-exception":
                            innerExceptions.Add(new ConnectDataException(ErrorCode.Unknown, new Exception()));
                            break;
                        case "any-exception":
                            innerExceptions.Add(new Exception());
                            break;
                    }
                }

                if (innerExceptions.Any())
                {
                    throw new ConnectAggregateException(ErrorCode.Unknown, false, innerExceptions.ToArray());
                }

                return Task.FromResult(batch);
            }
            
            _configurationProvider.GetRetriesConfig(Arg.Any<string>()).Returns(new RetryConfig {Attempts = 3, DelayTimeoutMs = 1});
            
            await Assert.ThrowsAsync<ConnectToleranceExceededException>(() =>
                _retriableHandler.Retry(Process, sinkRecordBatch, "connector"));
            
            Array.ForEach(expectedLogAttempts ?? Array.Empty<int>(),
                (i) => _sinkExceptionHandler.Received(1).LogRetryException(Arg.Any<ConnectException>(), i));
            Assert.Equal(expectedCalls, callCounter);
        }
        
        [Fact]
        public async Task Retry_SuccessOnFirstAttempt()
        {
            var sinkRecordBatch = GetBatch(2);
            var callCounter = 0;
            Task<SinkRecordBatch> Process(SinkRecordBatch batch)
            {
                ++callCounter;
                return Task.FromResult(batch);
            }
            
            _configurationProvider.GetRetriesConfig(Arg.Any<string>()).Returns(new RetryConfig {Attempts = 3, DelayTimeoutMs = 1});
            
            var actual = await _retriableHandler.Retry(Process, sinkRecordBatch, "connector");
            
            Assert.Same(sinkRecordBatch, actual);
            _sinkExceptionHandler.DidNotReceive().LogRetryException(Arg.Any<ConnectException>(), Arg.Any<int>());
            Assert.Equal(1, callCounter);
        }

        [Fact]
        public async Task Retry_SuccessAfterSplit()
        {
            var sinkRecordBatch = GetBatch(2, "retriable-exception", "data-exception");
            var callCounter = 0;

            Task<SinkRecordBatch> Process(SinkRecordBatch batch)
            {
                ++callCounter;
                var innerExceptions = new List<Exception>();
                foreach (var record in batch)
                {
                    switch (record.Topic)
                    {
                        case "retriable-exception":
                            innerExceptions.Add(new ConnectRetriableException(ErrorCode.Unknown, new Exception()));
                            break;
                        case "data-exception":
                            innerExceptions.Add(new ConnectDataException(ErrorCode.Unknown, new Exception()));
                            break;
                        case "any-exception":
                            innerExceptions.Add(new Exception());
                            break;
                    }
                }

                if (batch.Count > 1 && innerExceptions.Any())
                {
                    throw new ConnectAggregateException(ErrorCode.Unknown, false, innerExceptions.ToArray());
                }

                return Task.FromResult(batch);
            }

            _configurationProvider.GetRetriesConfig(Arg.Any<string>())
                .Returns(new RetryConfig {Attempts = 3, DelayTimeoutMs = 1});


            var actual = await _retriableHandler.Retry(Process, sinkRecordBatch, "connector");

            Assert.Same(sinkRecordBatch, actual);
            _sinkExceptionHandler.Received(1).LogRetryException(Arg.Any<ConnectException>(), 3);
            Assert.Equal(3, callCounter);
        }
        
        private static SinkRecordBatch GetBatch(int length = 2, params string[] topics)
        {
            var batch = new SinkRecordBatch("connector");

            for (var i = 0; i < length; i++)
            {
                var topic = topics != null && topics.Length > i ? topics[i] : string.Empty;
                batch.Add(new SinkRecord(new ConsumeResult<byte[], byte[]>
                    {Topic = topic, Message = new Message<byte[], byte[]>() {Headers = new Headers()}}, topic, 0, 0));
            }

            return batch;
        }
    }
}