using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Config;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Handlers
{
    public class PartitionHandlerTests
    {
        private readonly ILogger<PartitionHandler> _logger;
        private readonly IKafkaClientBuilder _kafkaClientBuilder;

        private readonly PartitionHandler _partitionHandler;

        public PartitionHandlerTests()
        {
            _logger = Substitute.For<ILogger<PartitionHandler>>();
            _kafkaClientBuilder = Substitute.For<IKafkaClientBuilder>();
            _partitionHandler = new PartitionHandler(_logger, _kafkaClientBuilder);
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(false, false)]
        public void CommitOffsets_When_NoOffsetsToCommit(bool isEmpty, bool canCommit)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>() {Headers = new Headers()}, Topic = "TopicA",
            })
            {
                CanCommitOffset = canCommit,
            };
            
            var batch = new SinkRecordBatch("commits");
            if (!isEmpty)
            {
                batch.Add(sinkRecord);
            }
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            
            _partitionHandler.CommitOffsets(batch, consumer, new ConnectorConfig());

            consumer.DidNotReceive().Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>());
            consumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [InlineData(false, true)]
        public void CommitOffsets_When_CommitOrStore(bool autoCommit, bool autoStore)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>
                {
                    Headers = new Headers()
                },
                TopicPartitionOffset = new TopicPartitionOffset("TopicA", new Partition(0), new Offset(10))
            })
            {
                CanCommitOffset = true,
            };

            var config = new ConnectorConfig()
            {
                EnableAutoCommit = autoCommit,
                EnableAutoOffsetStore = autoStore
            };

            var batch = new SinkRecordBatch("commits") {sinkRecord};
            var consumer = Substitute.For<IConsumer<byte[], byte[]>>();

            _partitionHandler.CommitOffsets(batch, consumer, config);

            consumer.Received(autoCommit ? 0 : 1)
                .Commit(Arg.Is<IEnumerable<TopicPartitionOffset>>(x => x.Any(t => t.Offset == 11)));
            consumer.Received(autoCommit && !autoStore ? 1 : 0)
                .StoreOffset(Arg.Is<TopicPartitionOffset>(t => t.Offset.Value == 11));
        }
        
        [Theory]
        [InlineData(true, false, null)]
        [InlineData(false, false, null)]
        [InlineData(false, true, null)]
        [InlineData(false, true, "")]
        public async Task NotifyEndOfPartition_When_EofNotSet(bool isnull, bool isEnabled, string topic)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>
                {
                    Headers = new Headers()
                },
                TopicPartitionOffset = new TopicPartitionOffset("TopicA", new Partition(0), new Offset(10))
            })
            {
                CanCommitOffset = true,
            };

            var config = new ConnectorConfig()
            {
                EndOfPartition = isnull ? null : new EndOfPartitionConfig() {Enabled = isEnabled, Topic = topic}
            };

            var batch = new SinkRecordBatch("commits") {sinkRecord};
            var producer = Substitute.For<IProducer<byte[], byte[]>>();
            _kafkaClientBuilder.GetProducer(Arg.Any<ConnectorConfig>()).Returns(producer);

            await _partitionHandler.NotifyEndOfPartition(batch, config);

            _kafkaClientBuilder.DidNotReceive().GetProducer(Arg.Any<ConnectorConfig>());
            await producer.DidNotReceive().ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>());
        }
        
        [Fact]
        public async Task NotifyEndOfPartition_When_EofPartitionNotFound()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>
                {
                    Headers = new Headers()
                },
                TopicPartitionOffset = new TopicPartitionOffset("TopicA", new Partition(0), new Offset(10))
            })
            {
                CanCommitOffset = true,
            };

            var config = new ConnectorConfig()
            {
                EndOfPartition =  new EndOfPartitionConfig() {Enabled = true, Topic = "topic"}
            };

            var batch = new SinkRecordBatch("commits") {sinkRecord};
            var producer = Substitute.For<IProducer<byte[], byte[]>>();
            _kafkaClientBuilder.GetProducer(Arg.Any<ConnectorConfig>()).Returns(producer);

            await _partitionHandler.NotifyEndOfPartition(batch, config);

            _kafkaClientBuilder.DidNotReceive().GetProducer(Arg.Any<ConnectorConfig>());
            await producer.DidNotReceive().ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>());
        }

        [Theory]
        [InlineData("eof-topic", 0, 5)]
        [InlineData("eof-topic-wrong", 1, 5)]
        [InlineData("eof-topic", 0, 10)]
        public async Task NotifyEndOfPartition_When_EofIsNotCommitted(string topic, int partition, int offset)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>
                {
                    Headers = new Headers()
                },
                TopicPartitionOffset = new TopicPartitionOffset(topic, new Partition(partition), new Offset(offset))
            })
            {
                CanCommitOffset = true,
            };

            var config = new ConnectorConfig()
            {
                EndOfPartition = new EndOfPartitionConfig() {Enabled = true, Topic = "topic"}
            };

            var batch = new SinkRecordBatch("commits") {sinkRecord};
            batch.SetPartitionEof(new TopicPartitionOffset("eof-topic", new Partition(1), new Offset(11)));
            var producer = Substitute.For<IProducer<byte[], byte[]>>();
            _kafkaClientBuilder.GetProducer(Arg.Any<ConnectorConfig>()).Returns(producer);

            await _partitionHandler.NotifyEndOfPartition(batch, config);

            _kafkaClientBuilder.DidNotReceive().GetProducer(Arg.Any<ConnectorConfig>());
            await producer.DidNotReceive().ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>());
        }
        
        [Fact]
        public async Task NotifyEndOfPartition_When_EofFound()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>
                {
                    Headers = new Headers()
                },
                TopicPartitionOffset = new TopicPartitionOffset("eof-topic", new Partition(1), new Offset(10))
            })
            {
                CanCommitOffset = true,
            };

            var config = new ConnectorConfig()
            {
                EndOfPartition = new EndOfPartitionConfig() {Enabled = true, Topic = "topic"}
            };

            var batch = new SinkRecordBatch("commits") {sinkRecord};
            batch.SetPartitionEof(new TopicPartitionOffset("eof-topic", new Partition(1), new Offset(11)));
            var producer = Substitute.For<IProducer<byte[], byte[]>>();
            _kafkaClientBuilder.GetProducer(Arg.Any<ConnectorConfig>()).Returns(producer);
            producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>())
                .Returns(new DeliveryReport<byte[], byte[]>(){ TopicPartitionOffset =new TopicPartitionOffset("eof-topic-1", new Partition(2), new Offset(110)) });
            
            await _partitionHandler.NotifyEndOfPartition(batch, config);

            _kafkaClientBuilder.Received().GetProducer(Arg.Any<ConnectorConfig>());
            await producer.Received().ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>());
        }
        
        [Fact]
        public async Task NotifyEndOfPartition_When_ProducerNotFound()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>
                {
                    Headers = new Headers()
                },
                TopicPartitionOffset = new TopicPartitionOffset("eof-topic", new Partition(1), new Offset(10))
            })
            {
                CanCommitOffset = true,
            };

            var config = new ConnectorConfig()
            {
                EndOfPartition = new EndOfPartitionConfig() {Enabled = true, Topic = "topic"}
            };

            var batch = new SinkRecordBatch("commits") {sinkRecord};
            batch.SetPartitionEof(new TopicPartitionOffset("eof-topic", new Partition(1), new Offset(11)));
            _kafkaClientBuilder.GetProducer(Arg.Any<ConnectorConfig>()).Returns((IProducer<byte[], byte[]>) null);
           
            await _partitionHandler.NotifyEndOfPartition(batch, config);

            _kafkaClientBuilder.Received().GetProducer(Arg.Any<ConnectorConfig>());
        }
    }
}