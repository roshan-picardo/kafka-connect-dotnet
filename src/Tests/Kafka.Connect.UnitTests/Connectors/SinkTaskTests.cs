using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Config;
using Kafka.Connect.Connectors;
using Kafka.Connect.Handlers;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Kafka.Connect.Tests.Connectors
{
    public class SinkTaskTests
    {
        private readonly IKafkaClientBuilder _kafkaClientBuilder;
        private readonly ISinkConsumer _sinkConsumer;
        private readonly ISinkProcessor _sinkProcessor;
        private readonly IPartitionHandler _partitionHandler;
        private readonly ISinkExceptionHandler _sinkExceptionHandler;
        private readonly IRetriableHandler _retriableHandler;
        private readonly SinkTask _sinkTask;
        private readonly IConsumer<byte[], byte[]> _consumer;

        public SinkTaskTests()
        {
            var logger = Substitute.For<ILogger<SinkTask>>();
            _kafkaClientBuilder = Substitute.For<IKafkaClientBuilder>();
            _sinkConsumer = Substitute.For<ISinkConsumer>();
            _sinkProcessor = Substitute.For<ISinkProcessor>();
            _partitionHandler = Substitute.For<IPartitionHandler>();
            _sinkExceptionHandler = Substitute.For<ISinkExceptionHandler>();
            _retriableHandler = Substitute.For<IRetriableHandler>();
            _consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            _sinkTask = new SinkTask(logger, _kafkaClientBuilder, _sinkConsumer, _sinkProcessor,
                _partitionHandler, _sinkExceptionHandler, _retriableHandler, null)
            {
            };
        }

        [Fact]
        public async Task Start_When_MethodsCalledInSequence()
        {
            _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
            var batch = new SinkRecordBatch("") {new SinkRecord(null)};
            _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
            _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            await _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
                Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>());

            await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

            //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
            _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
            await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
            await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.Received().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.Received().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            _sinkExceptionHandler.DidNotReceive().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
                Arg.Any<Action>());
            _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>());
            await _partitionHandler.DidNotReceive()
                .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        }

        [Fact]
        public async Task Start_When_ConsumeThrowsException()
        {
            _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
            var batch = new SinkRecordBatch("");
            _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Throws<Exception>();
            _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            await _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
                Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>());

            await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

            //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
            _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
            await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
            await _sinkProcessor.DidNotReceive().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            await _sinkProcessor.DidNotReceive().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.DidNotReceive().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.DidNotReceive().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            _sinkExceptionHandler.Received().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
                Arg.Any<Action>());
            _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>());
            await _partitionHandler.DidNotReceive()
                .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        }

        [Fact]
        public async Task Start_When_ProcessThrowsException()
        {
            _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
            var batch = new SinkRecordBatch("") {new SinkRecord(null)};
            _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
            _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Throws<Exception>();
            _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
                Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>()).Throws<Exception>();

            await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

            //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
            _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
            await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
            await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            await _sinkProcessor.DidNotReceive().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.DidNotReceive().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.DidNotReceive().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            _sinkExceptionHandler.Received().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
                Arg.Any<Action>());
            _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>());
            await _partitionHandler.DidNotReceive()
                .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        }

        [Fact]
        public async Task Start_When_SinkThrowsException()
        {
            _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
            var batch = new SinkRecordBatch("") {new SinkRecord(null)};
            _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
            _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Throws<Exception>();
            //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
                Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>()).Throws<Exception>();

            await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

            //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
            _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
            await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
            await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.DidNotReceive().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.DidNotReceive().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            _sinkExceptionHandler.Received().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
                Arg.Any<Action>());
            _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>());
            await _partitionHandler.DidNotReceive()
                .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        }

        [Fact]
        public async Task Start_When_StreamerProcessThrowsException()
        {
            _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
            var batch = new SinkRecordBatch("") {new SinkRecord(null)};
            _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
            _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Throws<Exception>();
            //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
                Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>()).Throws<Exception>();

            await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

           //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
            _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
            await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
            await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.Received().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.DidNotReceive().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            _sinkExceptionHandler.Received().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
                Arg.Any<Action>());
            _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>());
            await _partitionHandler.DidNotReceive()
                .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        }

        [Fact]
        public async Task Start_When_StreamerPublishThrowsException()
        {
            _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
            var batch = new SinkRecordBatch("") {new SinkRecord(null)};
            _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
            _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Throws<Exception>();
            _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
                Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>()).Throws<Exception>();

            await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

            //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
            _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
            await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
            await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.Received().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.Received().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            _sinkExceptionHandler.Received().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
                Arg.Any<Action>());
            _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>());
            await _partitionHandler.DidNotReceive()
                .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Start_When_BatchIsNullOrEmpty(bool isNull)
        {
            _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
            var batch = new SinkRecordBatch("unitTests") {new SinkRecord(null)};
            _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
            _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>())
               // .Returns(isNull ? null : batch);
            await _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
                Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>());

            await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

            //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
            _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
            await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
            await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.Received().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.Received().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            _sinkExceptionHandler.DidNotReceive().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
                Arg.Any<Action>());
            _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>());
            await _partitionHandler.DidNotReceive()
                .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        }

        [Fact]
        public async Task Start_When_BatchIsNotEmpty()
        {
            _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns(_consumer);
            var batch = new SinkRecordBatch("")
            {
                new ConsumeResult<byte[], byte[]>()
                {
                    Message = new Message<byte[], byte[]>()
                    {
                        Headers = new Headers(), Timestamp = new Timestamp(DateTime.Now)
                    }
                }
            };
            _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
            _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
                Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>()).Returns(batch);

            await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

            //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
            _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
            await _sinkConsumer.Received().Consume(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
            await _sinkProcessor.Received().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            await _sinkProcessor.Received().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.Received().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.Received().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            _sinkExceptionHandler.DidNotReceive().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
                Arg.Any<Action>());
            _partitionHandler.Received().CommitOffsets(Arg.Any<SinkRecordBatch>(), _consumer,
                Arg.Any<ConnectorConfig>());
            await _partitionHandler.Received().NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        }

        [Fact]
        public async Task Start_When_ConsumerIsNull()
        {
            _sinkConsumer.Subscribe(Arg.Any<ConnectorConfig>()).Returns((IConsumer<byte[], byte[]>) null);
            var batch = new SinkRecordBatch("");
            _sinkConsumer.Consume(Arg.Any<SinkRecordBatch>(), Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>()).Returns(batch);
            _sinkProcessor.Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            _sinkProcessor.Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Returns(batch);
            //_messageStreamer.Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>()).Throws<Exception>();
            await _retriableHandler.Retry(Arg.Do<Func<SinkRecordBatch, Task<SinkRecordBatch>>>(async f => batch = await f.Invoke(batch)),
                Arg.Any<SinkRecordBatch>(), Arg.Any<int>(), Arg.Any<int>());

            await _sinkTask.Start(new ConnectorConfig(), new CancellationTokenSource());

            //_kafkaClientBuilder.AttachPartitionChangeEvents(Arg.Any<TaskContext>());
            _sinkConsumer.Received().Subscribe(Arg.Any<ConnectorConfig>());
            await _sinkConsumer.DidNotReceive().Consume(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>(), Arg.Any<BatchPollContext>());
            await _sinkProcessor.DidNotReceive().Process(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            await _sinkProcessor.DidNotReceive().Sink(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.DidNotReceive().Enrich(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            //await _messageStreamer.DidNotReceive().Publish(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
            _sinkExceptionHandler.DidNotReceive().Handle(Arg.Any<ConnectorConfig>(), Arg.Any<Exception>(),
                Arg.Any<Action>());
            _partitionHandler.DidNotReceive().CommitOffsets(Arg.Any<SinkRecordBatch>(),
                Arg.Any<IConsumer<byte[], byte[]>>(),
                Arg.Any<ConnectorConfig>());
            await _partitionHandler.DidNotReceive()
                .NotifyEndOfPartition(Arg.Any<SinkRecordBatch>(), Arg.Any<ConnectorConfig>());
        }
    }
}