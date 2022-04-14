using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Handlers
{
    public class RetriableHandlerTests
    {
        private readonly ILogger<RetriableHandler> _logger;
        private readonly IRetriableHandler _retriableHandler;

        public RetriableHandlerTests()
        {
            _logger = Substitute.For<ILogger<RetriableHandler>>();
            _retriableHandler = new RetriableHandler(_logger);
        }

        [Fact]
        public async Task Retry_SinkBatch_When_Returns_OK()
        {
            var sinkRecordBatch = new SinkRecordBatch("");
            Task<SinkRecordBatch> Consume() => Task.FromResult(sinkRecordBatch);
            
            var actual = await _retriableHandler.Retry(Consume   , 3, 1000);
            
            Assert.NotNull(actual);
            Assert.Equal(sinkRecordBatch, actual);
        }

        [Fact]
        public async Task Retry_SinkBatch_Throws_ConnectAggregateException_WithNoInnerExceptions()
        {
            var sinkRecordBatch = new SinkRecordBatch("");
            static Task<Guid> Process(SinkRecordBatch batch) => throw new ConnectAggregateException(ErrorCode.Unknown);

            await Assert.ThrowsAsync<ConnectToleranceExceededException>(async () =>
                await _retriableHandler.Retry((b) => Process(sinkRecordBatch), sinkRecordBatch, 1, 1000));
        }
        
        [Fact]
        public async Task Retry_SinkBatch_Throws_ConnectAggregateException_WithRetriable_NoAttemptsLeft()
        {
            var sinkRecordBatch = new SinkRecordBatch("");

            static Task<Guid> Process(SinkRecordBatch batch) =>
                throw new ConnectAggregateException(ErrorCode.Unknown,
                    new ConnectRetriableException(ErrorCode.Unknown, new Exception()));

            await Assert.ThrowsAsync<ConnectToleranceExceededException>(async () =>
                await _retriableHandler.Retry((b) => Process(sinkRecordBatch), sinkRecordBatch, 0, 1000));
        }
        
        [Fact]
        public async Task Retry_SinkBatch_Throws_ConnectAggregateException_WithRetriable_SplitBatch()
        {
            var sinkRecordBatch = new SinkRecordBatch("")
            {
                new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>(){Headers = new Headers()}}),
                new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>(){Headers = new Headers()}}),
            };

            var counter = 0;

            Task<Guid> Process(SinkRecordBatch batch)
            {
                ++counter;
                throw new ConnectAggregateException(ErrorCode.Unknown, new ConnectRetriableException(ErrorCode.Unknown, new Exception()));
            }

            await Assert.ThrowsAsync<ConnectToleranceExceededException>(async () =>
                await _retriableHandler.Retry(b => Process(sinkRecordBatch), sinkRecordBatch, 3, 1000));
            Assert.Equal(4, counter);
        }

        [Fact]
        public async Task Retry_SinkBatch_Throws_ConnectAggregateException_WithPartialRetry_SplitBatch()
        {
            var sinkRecordBatch = new SinkRecordBatch("")
            {
                new SinkRecord(new ConsumeResult<byte[], byte[]>()
                    {Topic = "Retry", Message = new Message<byte[], byte[]>() {Headers = new Headers()}}),
                new SinkRecord(new ConsumeResult<byte[], byte[]>()
                    {Message = new Message<byte[], byte[]>() {Headers = new Headers()}}),
            };

            var counter = 0;

            Task<Guid> Process(SinkRecordBatch batch)
            {
                ++counter;
                if (batch.Count == 2)
                {
                    throw new ConnectAggregateException(ErrorCode.Unknown, false,
                        new ConnectRetriableException(ErrorCode.Unknown, new Exception()),
                        new ConnectDataException(ErrorCode.Unknown, new Exception()));
                }
                if (batch.First().Topic == "Retry")
                {
                    throw new ConnectAggregateException(ErrorCode.Unknown, false,
                        new ConnectRetriableException(ErrorCode.Unknown, new Exception()));
                }

                throw new ConnectAggregateException(ErrorCode.Unknown, false,
                    new ConnectDataException(ErrorCode.Unknown, new Exception()));
            }

            await Assert.ThrowsAsync<ConnectToleranceExceededException>(async () =>
                await _retriableHandler.Retry(Process, sinkRecordBatch, 3, 1000));
            Assert.Equal(3, counter);
        }
        
        [Fact]
        public async Task Retry_SinkBatch_Throws_ConnectAggregateException_WithPartialData_SplitBatch()
        {
            var sinkRecordBatch = new SinkRecordBatch("")
            {
                new SinkRecord(new ConsumeResult<byte[], byte[]>()
                    {Topic = "Data", Message = new Message<byte[], byte[]>() {Headers = new Headers()}}),
                new SinkRecord(new ConsumeResult<byte[], byte[]>()
                    {Message = new Message<byte[], byte[]>() {Headers = new Headers()}}),
            };

            var counter = 0;

            Task<Guid> Process(SinkRecordBatch batch)
            {
                ++counter;
                if (batch.Count == 2)
                {
                    throw new ConnectAggregateException(ErrorCode.Unknown, false,
                        new ConnectRetriableException(ErrorCode.Unknown, new Exception()),
                        new ConnectDataException(ErrorCode.Unknown, new Exception()));
                }
                if (batch.First().Topic == "Data")
                {
                    throw new ConnectAggregateException(ErrorCode.Unknown, false,
                        new ConnectDataException(ErrorCode.Unknown, new Exception()));
                }

                throw new ConnectAggregateException(ErrorCode.Unknown, false,
                    new ConnectRetriableException(ErrorCode.Unknown, new Exception()));
            }

            await Assert.ThrowsAsync<ConnectToleranceExceededException>(async () =>
                await _retriableHandler.Retry(Process, sinkRecordBatch, 3, 1000));
            Assert.Equal(2, counter);
        }
        
        [Fact]
        public async Task Retry_SinkBatch_Throws_ConnectAggregateException_WithData_NoSplit()
        {
            var sinkRecordBatch = new SinkRecordBatch("")
            {
                new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>(){Headers = new Headers()}}) { Status = SinkStatus.Failed},
                new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>(){Headers = new Headers()}}) { Status = SinkStatus.Failed},
            };

            var counter = 0;

            Task<Guid> Process(SinkRecordBatch batch)
            {
                ++counter;
                throw new ConnectAggregateException(ErrorCode.Unknown, false, new ConnectDataException(ErrorCode.Unknown, new Exception()), new ConnectDataException(ErrorCode.Unknown, new Exception()));
            }

            await Assert.ThrowsAsync<ConnectToleranceExceededException>(async () =>
                await _retriableHandler.Retry(b => Process(sinkRecordBatch), sinkRecordBatch, 3, 1000));
            Assert.Equal(1, counter);
        }
        
        [Fact]
        public async Task Retry_SinkBatch_Throws_ConnectAggregateException_WithRetriableAndNonConnect_NoSplit()
        {
            var sinkRecordBatch = new SinkRecordBatch("")
            {
                new SinkRecord(new ConsumeResult<byte[], byte[]>(){Topic = "Retry", Message = new Message<byte[], byte[]>(){Headers = new Headers()}}),
                new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>(){Headers = new Headers()}}),
            };

            var counter = 0;

            Task<Guid> Process(SinkRecordBatch batch)
            {
                ++counter;
                if (batch.Count == 2)
                {
                    throw new ConnectAggregateException(ErrorCode.Unknown, false,
                        new ConnectRetriableException(ErrorCode.Unknown, new Exception()),
                        new Exception());
                }
                if (batch.First().Topic == "Retry")
                {
                    throw new ConnectAggregateException(ErrorCode.Unknown, false,
                        new ConnectRetriableException(ErrorCode.Unknown, new Exception()));
                }

                throw new ConnectAggregateException(ErrorCode.Unknown, false,
                    new Exception());
            }

            await Assert.ThrowsAsync<ConnectToleranceExceededException>(async () =>
                await _retriableHandler.Retry(Process, sinkRecordBatch, 3, 1000));
            Assert.Equal(3, counter);
        }
        
        [Fact]
        public async Task Retry_SinkBatch_Throws_ConnectRetriableException_WithRetries()
        {
            var sinkRecordBatch = new SinkRecordBatch("")
            {
                new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>(){Headers = new Headers()}}),
            };

            var counter = 0;

            Task<Guid> Process(SinkRecordBatch batch)
            {
                ++counter;
                throw new ConnectRetriableException(ErrorCode.Unknown, new Exception());
            }

            await Assert.ThrowsAsync<ConnectToleranceExceededException>(async () =>
                await _retriableHandler.Retry(Process, sinkRecordBatch, 3, 1000));
            Assert.Equal(4, counter);
        }
        
        [Fact]
        public async Task Retry_SinkBatch_Throws_ConnectDataException()
        {
            var sinkRecordBatch = new SinkRecordBatch("")
            {
                new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>(){Headers = new Headers()}}),
            };

            var counter = 0;

            Task<Guid> Process(SinkRecordBatch batch)
            {
                ++counter;
                throw new ConnectDataException(ErrorCode.Unknown, new Exception());
            }

            await Assert.ThrowsAsync<ConnectToleranceExceededException>(async () =>
                await _retriableHandler.Retry(Process, sinkRecordBatch, 3, 1000));
            Assert.Equal(1, counter);
        }
        
        [Fact]
        public async Task Retry_SinkBatch_Throws_AnyException()
        {
            var sinkRecordBatch = new SinkRecordBatch("")
            {
                new SinkRecord(new ConsumeResult<byte[], byte[]>(){Message = new Message<byte[], byte[]>(){Headers = new Headers()}}),
            };

            var counter = 0;

            Task<Guid> Process(SinkRecordBatch batch)
            {
                ++counter;
                throw new Exception();
            }

            await Assert.ThrowsAsync<ConnectToleranceExceededException>(async () =>
                await _retriableHandler.Retry(Process, sinkRecordBatch, 3, 1000));
            Assert.Equal(1, counter);
        }
    }
}