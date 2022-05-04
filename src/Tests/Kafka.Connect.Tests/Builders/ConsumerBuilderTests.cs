using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Builders
{
    public class ConsumerBuilderTests
    {
        private readonly ILogger<ConsumerBuilder> _logger;
        private readonly IExecutionContext _executionContext;
        private readonly IConfigurationProvider _configurationProvider;
        private ConsumerBuilder _consumerBuilder;

        public ConsumerBuilderTests()
        {
            _logger = Substitute.For<MockLogger<ConsumerBuilder>>();
            _executionContext = Substitute.For<IExecutionContext>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
        }

        [Fact]
        public void Build_ReturnsValidConsumer()
        {
            Assert.NotNull(BuildConsumer());
        }

        [Theory]
        [MemberData(nameof(GetErrors))]
        public void Build_ErrorHandlerReturningExpectedLogs(LogLevel level, Error error)
        {
            var consumer = BuildConsumer();
            
            _consumerBuilder.ErrorHandler.Invoke(consumer, error);
            
            _logger.Received().Log(level, "{@Log}", error);
        }
        
        public static IEnumerable<object[]> GetErrors
        {
            get
            {
                yield return new object[] { LogLevel.Critical, new Error(ErrorCode.Unknown, "", true) };
                yield return new object[] { LogLevel.Error, new Error(ErrorCode.Unknown)};
                yield return new object[] { LogLevel.Error, new Error(ErrorCode.OffsetOutOfRange)};
                yield return new object[] { LogLevel.Error, new Error(ErrorCode.Local_Application)};
                yield return new object[] { LogLevel.Debug, new Error(ErrorCode.NoError)};
            }
        }
        
        [Theory]
        [InlineData(SyslogLevel.Critical, LogLevel.Critical)]
        [InlineData(SyslogLevel.Emergency, LogLevel.Critical)]
        [InlineData(SyslogLevel.Alert, LogLevel.Error)]
        [InlineData(SyslogLevel.Error, LogLevel.Error)]
        [InlineData(SyslogLevel.Notice, LogLevel.Warning)]
        [InlineData(SyslogLevel.Warning, LogLevel.Warning)]
        [InlineData(SyslogLevel.Info, LogLevel.Information)]
        [InlineData(SyslogLevel.Debug, LogLevel.Debug)]
        [InlineData((SyslogLevel)10, LogLevel.Warning)]
        public void Build_LogHandlerReturningExpectedLogs(SyslogLevel syslogLevel, LogLevel logLevel)
        {
            var consumer = BuildConsumer();
            var log = new LogMessage("Test", syslogLevel, "tests", "Unit tests logs");
            
            _consumerBuilder.LogHandler.Invoke(consumer, log);
            
            _logger.Received().Log(logLevel, "{@Log}", log);
        }
        
        [Fact]
        public void Build_StatisticsHandlerReturningExpectedLogs()
        {
            var consumer = BuildConsumer();
            const string log = "unit test stats";
            
            _consumerBuilder.StatisticsHandler.Invoke(consumer, log);
            
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new { Message = "Statistics", Stats = log });
        }
        
        [Fact]
        public void Build_PartitionAssignedHandlerReturningExpectedLogs_WhenNoPartitionsSet()
        {
            var consumer = BuildConsumer();
            
            _consumerBuilder.PartitionsAssignedHandler.Invoke(consumer, new List<TopicPartition>());
            
            _logger.Received().Log(LogLevel.Trace, "{@Log}", new { Message = "No partitions assigned." });
        }
        
        [Fact]
        public void Build_PartitionAssignedHandlerReturningExpectedLogs_WhenPartitionListIsSet()
        {
            var consumer = BuildConsumer();
            var partitions = new List<TopicPartition> {new("test-one", 0), new("test-one", 1), new("test-2", 0)};
            
            _consumerBuilder.PartitionsAssignedHandler.Invoke(consumer, partitions);
            
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new { Message="Assigned partitions.", Partitions = partitions.Select(p=>$"{{Topic:{p.Topic}}} - {{Partition:{p.Partition.Value}}}").ToList()});
        }
        
        [Fact]
        public void Build_PartitionAssignedHandlerReturningExpectedLogs_InvokesOnPartitionAssigned()
        {
            var consumer = BuildConsumer();
            _consumerBuilder.AttachPartitionChangeEvents("test-connector", 1);
            var partitions = new List<TopicPartition> {new("test-one", 0), new("test-one", 1), new("test-2", 0)};
            
            _consumerBuilder.PartitionsAssignedHandler.Invoke(consumer, partitions);
            
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new { Message="Assigned partitions.", Partitions = partitions.Select(p=>$"{{Topic:{p.Topic}}} - {{Partition:{p.Partition.Value}}}").ToList()});
            _executionContext.Received().AssignPartitions("test-connector", 1, partitions);
        }
        
        [Fact]
        public void Build_PartitionRevokedHandlerReturningExpectedLogs_WhenNoPartitionsSet()
        {
            var consumer = BuildConsumer();
            
            _consumerBuilder.PartitionsRevokedHandler.Invoke(consumer, new List<TopicPartitionOffset>());
            
            _logger.Received().Log(LogLevel.Trace, "{@Log}", new { Message = "No partitions revoked." });
        }
        
        [Fact]
        public void Build_PartitionRevokedHandlerReturningExpectedLogs_WhenPartitionListIsSet()
        {
            var consumer = BuildConsumer();
            var offsets = new List<TopicPartitionOffset> {new("test-one", 0, 1122), new("test-one", 1, 98192), new("test-2", 0, 0)};

            _consumerBuilder.PartitionsRevokedHandler.Invoke(consumer, offsets);
            
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new { Message="Revoked partitions.", Partitions = offsets.Select(p=> $"{{topic={p.Topic}}} - {{partition={p.Partition.Value}}} - {{offset:{p.Offset.Value}}}").ToList()});
        }
        
        [Fact]
        public void Build_PartitionRevokedHandlerReturningExpectedLogs_InvokesOnPartitionRevoked()
        {
            var consumer = BuildConsumer();
            var offsets = new List<TopicPartitionOffset> {new("test-one", 0, 1122), new("test-one", 1, 98192), new("test-2", 0, 0)};
            _consumerBuilder.AttachPartitionChangeEvents("test-connector", 1);

            _consumerBuilder.PartitionsRevokedHandler.Invoke(consumer, offsets);

            _executionContext.Received().RevokePartitions("test-connector", 1, Arg.Any<IEnumerable<TopicPartition>>());
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new { Message="Revoked partitions.", Partitions = offsets.Select(p=> $"{{topic={p.Topic}}} - {{partition={p.Partition.Value}}} - {{offset:{p.Offset.Value}}}").ToList()});
            
        }
        
        [Fact]
        public void Build_OffsetsCommittedHandlerReturningExpectedLogs_WhenNoOffsets()
        {
            var consumer = BuildConsumer();

            _consumerBuilder.OffsetsCommittedHandler.Invoke(consumer, new CommittedOffsets(new List<TopicPartitionOffsetError>(), ErrorCode.NoError));
            
            _logger.Received().Log(LogLevel.Trace, "{@Log}", new { Message = "No offsets committed." });
        }
        
        [Fact]
        public void Build_OffsetsCommittedHandlerReturningExpectedLogs_WhenCommitErrored()
        {
            var consumer = BuildConsumer();

            var error = new Error(ErrorCode.OffsetOutOfRange, "commit failed");
            _consumerBuilder.OffsetsCommittedHandler.Invoke(consumer, new CommittedOffsets(new List<TopicPartitionOffsetError>(), error));
            
            _logger.Received().Log(LogLevel.Warning, "{@Log}", new { Message = "Error committing offsets.", Reason=error, Offsets = new List<TopicPartitionOffsetError>().Select(p=> $"{{topic={p.Topic}}} - {{partition={p.Partition.Value}}} - {{offset:{p.Offset.Value}}}").ToList()});
        }
        
        [Fact]
        public void Build_OffsetsCommittedHandlerReturningExpectedLogs_WhenCommitErroredWithOffsets()
        {
            var consumer = BuildConsumer();

            var error = new Error(ErrorCode.OffsetOutOfRange, "commit failed");
            _consumerBuilder.OffsetsCommittedHandler.Invoke(consumer, new CommittedOffsets(new List<TopicPartitionOffsetError> { new("topic", 1, 1000, error) }, error));
            
            _logger.Received().Log(LogLevel.Warning, "{@Log}", new { Message = "Error committing offsets.", Reason=error, Offsets = new List<TopicPartitionOffsetError>().Select(p=> $"{{topic={p.Topic}}} - {{partition={p.Partition.Value}}} - {{offset:{p.Offset.Value}}}").ToList()});
        }
        
        [Fact]
        public void Build_PartitionRevokedHandlerReturningExpectedLogs_WhenOffsetsCommitted()
        {
            var consumer = BuildConsumer();
            var offsets =
                new CommittedOffsets(new List<TopicPartitionOffsetError> {new("topic", 0, 1123, ErrorCode.NoError), new("topic", 5, 1123, ErrorCode.NoError)}, ErrorCode.NoError);
            
            _consumerBuilder.OffsetsCommittedHandler.Invoke(consumer, offsets);
            
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new { Message="Offsets committed.", Offsets = offsets.Offsets.Select(p=> $"{{topic={p.Topic}}} - {{partition={p.Partition.Value}}} - {{offset:{p.Offset.Value}}}").ToList()});
        }
        
        private IConsumer<byte[], byte[]> BuildConsumer()
        {
            _configurationProvider.GetConsumerConfig(Arg.Any<string>())
                .Returns(new ConsumerConfig() {BootstrapServers = "localhost:9092", GroupId = "test-group"});
            _consumerBuilder = new ConsumerBuilder(_logger, _configurationProvider, _executionContext, "");
            return _consumerBuilder.Create();
        }
    }
}