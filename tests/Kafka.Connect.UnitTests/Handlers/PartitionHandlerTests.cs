using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Handlers
{
    public class PartitionHandlerTests
    {
        private readonly ILogger<PartitionHandler> _logger;
        private readonly IKafkaClientBuilder _kafkaClientBuilder;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IConsumer<byte[], byte[]> _consumer;
        private readonly IProducer<byte[], byte[]> _producer;

        private readonly PartitionHandler _partitionHandler;

        public PartitionHandlerTests()
        {
            _logger = Substitute.For<ILogger<PartitionHandler>>();
            _kafkaClientBuilder = Substitute.For<IKafkaClientBuilder>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _consumer = Substitute.For<IConsumer<byte[], byte[]>>();
            _producer = Substitute.For<IProducer<byte[], byte[]>>();
            _partitionHandler = new PartitionHandler(_logger, _kafkaClientBuilder, _configurationProvider);
        }
        
        [Theory]
        [InlineData(true, true)]
        [InlineData(false, false)]
        public void CommitOffsets_WhenNoOffsetsToCommit(bool isEmpty, bool canCommit)
        {
            var batch = new ConnectRecordBatch("commits");
            if (!isEmpty)
            {
                var record = GetRecord("topicA", 0, 10);
                record.CanCommitOffset = canCommit;
                batch.Add(record);
            }
            
            _partitionHandler.CommitOffsets(batch, _consumer);

            _consumer.DidNotReceive().Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>());
            _consumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
            _configurationProvider.DidNotReceive().GetAutoCommitConfig();
        }
        

        [Theory]
        [InlineData(false, false, 1, 0)]
        [InlineData(true, false, 0, 1)]
        [InlineData(true, true, 0, 0)]
        [InlineData(false, true, 1, 0)]
        public void CommitOffsets_WhenCommitOrStore(bool enableAutoCommit, bool enableAutoOffsetStore, int callCountCommit, int callCountStoreOffset)
        {
            _configurationProvider.GetAutoCommitConfig().Returns((enableAutoCommit, enableAutoOffsetStore));
            var batch = new ConnectRecordBatch("commits") {GetRecord("topicA", 0, 10)};

            _partitionHandler.CommitOffsets(batch, _consumer);

            _consumer.Received(callCountCommit)
                .Commit(Arg.Is<IEnumerable<TopicPartitionOffset>>(x => x.Any(t => t.Offset == 11)));
            _consumer.Received(callCountStoreOffset)
                .StoreOffset(Arg.Is<TopicPartitionOffset>(t => t.Offset.Value == 11));
        }
        
        [Fact]
        public void CommitOffsets_OffsetStoreInvokedForEveryTopicPartition()
        {
            _configurationProvider.GetAutoCommitConfig().Returns((true, false));

            var batch = new ConnectRecordBatch("commits")
            {
                GetRecord("topicA", 0, 10), 
                GetRecord("topicA", 0, 100), 
                GetRecord("topicA", 1, 3),
                GetRecord("topicB", 0, 10)
            };

            _partitionHandler.CommitOffsets(batch, _consumer);

            _consumer.Received(2).StoreOffset(Arg.Is<TopicPartitionOffset>(t => t.Topic == "topicA"));
            _consumer.Received(1).StoreOffset(Arg.Is<TopicPartitionOffset>(t => t.Topic == "topicB"));
        }
        
        [Theory]
        [InlineData(true, false, null)]
        [InlineData(false, false, null)]
        [InlineData(false, true, null)]
        [InlineData(false, true, "")]
        public async Task NotifyEndOfPartition_WhenEofConfigNotSet(bool isnull, bool isEnabled, string topic)
        {
            _configurationProvider.GetEofSignalConfig(Arg.Any<string>()).Returns((_) => isnull ? null : new EofConfig {Enabled = isEnabled, Topic = topic});

            var batch = new ConnectRecordBatch("commits") {GetRecord("topicA", 0, 10)};

            await _partitionHandler.NotifyEndOfPartition(batch, "connector", 1);

            _kafkaClientBuilder.DidNotReceive().GetProducer(Arg.Any<string>());
            await _producer.DidNotReceive().ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>());
        }
        
        [Fact]
        public async Task NotifyEndOfPartition_WhenNoEofPartitionsSet()
        {
            _configurationProvider.GetEofSignalConfig(Arg.Any<string>()).Returns(new EofConfig {Enabled = true, Topic = "eof-topic"});

            var batch = new ConnectRecordBatch("commits") {GetRecord("topicA", 0, 10)};

            await _partitionHandler.NotifyEndOfPartition(batch, "connector", 1);

            _kafkaClientBuilder.DidNotReceive().GetProducer(Arg.Any<string>());
            await _producer.DidNotReceive().ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>());
        }
        
        [Theory]
        [InlineData("data-topic", 0, 101)]
        [InlineData("data-topic", 1, 100)]
        [InlineData("wrong-topic", 0, 100)]
        public async Task NotifyEndOfPartition_WhenEofPartitionsNotPresentInList(string topic, int partition, int offset)
        {
            _configurationProvider.GetEofSignalConfig(Arg.Any<string>()).Returns(new EofConfig {Enabled = true, Topic = "eof-topic"});

            var batch = new ConnectRecordBatch("commits") {GetRecord(topic, partition, offset)};
            batch.SetPartitionEof("data-topic", 0, 101);

            await _partitionHandler.NotifyEndOfPartition(batch, "connector", 1);

            _kafkaClientBuilder.DidNotReceive().GetProducer(Arg.Any<string>());
            await _producer.DidNotReceive().ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>());
        }
        
        [Fact]
        public async Task NotifyEndOfPartition_WhenProducerNotConfigured()
        {
            _configurationProvider.GetEofSignalConfig(Arg.Any<string>()).Returns(new EofConfig {Enabled = true, Topic = "eof-topic"});

            var batch = new ConnectRecordBatch("commits") {GetRecord("data-topic", 0, 100)};
            batch.SetPartitionEof("data-topic", 0, 101);
            _kafkaClientBuilder.GetProducer(Arg.Any<string>()).Returns((IProducer<byte[], byte[]>) null);

            await _partitionHandler.NotifyEndOfPartition(batch, "connector", 1);

            _kafkaClientBuilder.Received().GetProducer(Arg.Any<string>());
            _logger.Received().Warning("No producer configured to publish EOF message.");

        }
        
        [Fact]
        public async Task NotifyEndOfPartition_SendsMessageToEofTopic()
        {
            _configurationProvider.GetEofSignalConfig(Arg.Any<string>()).Returns(new EofConfig {Enabled = true, Topic = "eof-topic"});

            var batch = new ConnectRecordBatch("commits") {GetRecord("data-topic", 0, 100)};
            batch.SetPartitionEof("data-topic", 0, 101);
            _kafkaClientBuilder.GetProducer(Arg.Any<string>()).Returns(_producer);
            var delivered = new DeliveryResult<byte[], byte[]>
            {
                Topic = "eof-topic",
                Partition = 0,
                Offset = 10
            };
            _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>()).Returns(delivered);
            
            await _partitionHandler.NotifyEndOfPartition(batch, "connector", 1);

            _kafkaClientBuilder.Received().GetProducer(Arg.Any<string>());
            await _producer.Received().ProduceAsync("eof-topic", Arg.Any<Message<byte[], byte[]>>());
            _logger.Received().Info( "EOF message delivered.", Arg.Any<object>());
        }
        
        private static ConnectRecord GetRecord(string topic, int partition, int offset)
        {
            return  new global::Kafka.Connect.Models.SinkRecord(new ConsumeResult<byte[], byte[]>
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
        }
    }
}