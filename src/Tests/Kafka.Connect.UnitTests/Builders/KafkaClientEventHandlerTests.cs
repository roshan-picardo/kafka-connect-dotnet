using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Connectors;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Builders
{
    public class KafkaClientEventHandlerTests
    {
        private readonly ILogger<KafkaClientEventHandler> _logger;
        private readonly IExecutionContext _executionContext;
        private readonly KafkaClientEventHandler _kafkaClientEventHandler;

        public KafkaClientEventHandlerTests()
        {
            _logger = Substitute.For<MockLogger<KafkaClientEventHandler>>();
            _executionContext = Substitute.For<IExecutionContext>();

            _kafkaClientEventHandler = new KafkaClientEventHandler(_logger, _executionContext);
        }
        
        [Theory]
        [MemberData(nameof(GetErrors))]
        public void HandleError_ReturningExpectedLogs(LogLevel level, Error error)
        {
            _kafkaClientEventHandler.HandleError(error);
            
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
        public void HandleLogMessage_ReturningExpectedLogs(SyslogLevel syslogLevel, LogLevel logLevel)
        {
            var log = new LogMessage("Test", syslogLevel, "tests", "Unit tests logs");
            
            _kafkaClientEventHandler.HandleLogMessage(log);
            
            _logger.Received().Log(logLevel, "{@Log}", log);
        }
        
        [Fact]
        public void HandleStatistics_ReturningExpectedLogs()
        {
            const string log = "unit test stats";
            
            _kafkaClientEventHandler.HandleStatistics(log);
            
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new { Message = "Statistics", Stats = log });
        }
        
        [Fact]
        public void HandlePartitionAssigned_WhenNoPartitionsSet()
        {
            
            _kafkaClientEventHandler.HandlePartitionAssigned("connector", 1, new List<TopicPartition>());
            
            _logger.Received().Log(LogLevel.Trace, "{@Log}", new { Message = "No partitions assigned." });
            _executionContext.DidNotReceive().AssignPartitions(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<List<TopicPartition>>());
        }
        
        
        [Fact]
        public void HandlePartitionAssigned_InvokesOnPartitionAssigned()
        {
            var partitions = new List<TopicPartition> {new("test-one", 0), new("test-one", 1), new("test-2", 0)};
            
            _kafkaClientEventHandler.HandlePartitionAssigned("connector", 1, partitions);
            
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new { Message="Assigned partitions.", Partitions = partitions.Select(p=>$"{{Topic:{p.Topic}}} - {{Partition:{p.Partition.Value}}}").ToList()});
            _executionContext.Received().AssignPartitions("connector", 1, partitions);
        }
        
        [Fact]
        public void HandlePartitionRevoked_WhenNoPartitionsSet()
        {
            _kafkaClientEventHandler.HandlePartitionRevoked("connector", 1, new List<TopicPartitionOffset>());
            
            _logger.Received().Log(LogLevel.Trace, "{@Log}", new { Message = "No partitions revoked." });
            _executionContext.DidNotReceive().RevokePartitions(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<List<TopicPartition>>());
        }
        
        [Fact]
        public void HandlePartitionRevoked_InvokesOnPartitionRevoked()
        {
            var offsets = new List<TopicPartitionOffset> {new("test-one", 0, 1122), new("test-one", 1, 98192), new("test-2", 0, 0)};
            _kafkaClientEventHandler.HandlePartitionRevoked("connector", 1, offsets);

            _executionContext.Received().RevokePartitions("connector", 1, Arg.Any<IEnumerable<TopicPartition>>());
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new { Message="Revoked partitions.", Partitions = offsets.Select(p=> $"{{topic={p.Topic}}} - {{partition={p.Partition.Value}}} - {{offset:{p.Offset.Value}}}").ToList()});
            
        }
        
        [Fact]
        public void Build_OffsetsCommittedHandlerReturningExpectedLogs_WhenNoOffsets()
        {
            _kafkaClientEventHandler.HandleOffsetCommitted(new CommittedOffsets(new List<TopicPartitionOffsetError>(), ErrorCode.NoError));
            
            _logger.Received().Log(LogLevel.Trace, "{@Log}", new { Message = "No offsets committed." });
        }
        
        [Fact]
        public void Build_OffsetsCommittedHandlerReturningExpectedLogs_WhenCommitErrored()
        {
            var error = new Error(ErrorCode.OffsetOutOfRange, "commit failed");
            _kafkaClientEventHandler.HandleOffsetCommitted(new CommittedOffsets(new List<TopicPartitionOffsetError>(), error));
            
            _logger.Received().Log(LogLevel.Warning, "{@Log}", new { Message = "Error committing offsets.", Reason=error, Offsets = new List<TopicPartitionOffsetError>().Select(p=> $"{{topic={p.Topic}}} - {{partition={p.Partition.Value}}} - {{offset:{p.Offset.Value}}}").ToList()});
        }
        
        [Fact]
        public void Build_OffsetsCommittedHandlerReturningExpectedLogs_WhenCommitErroredWithOffsets()
        {
            var error = new Error(ErrorCode.OffsetOutOfRange, "commit failed");
            _kafkaClientEventHandler.HandleOffsetCommitted(new CommittedOffsets(new List<TopicPartitionOffsetError> { new("topic", 1, 1000, error) }, error));
            
            _logger.Received().Log(LogLevel.Warning, "{@Log}", new { Message = "Error committing offsets.", Reason=error, Offsets = new List<TopicPartitionOffsetError>().Select(p=> $"{{topic={p.Topic}}} - {{partition={p.Partition.Value}}} - {{offset:{p.Offset.Value}}}").ToList()});
        }
        
        [Fact]
        public void Build_PartitionRevokedHandlerReturningExpectedLogs_WhenOffsetsCommitted()
        {
            var offsets =
                new CommittedOffsets(new List<TopicPartitionOffsetError> {new("topic", 0, 1123, ErrorCode.NoError), new("topic", 5, 1123, ErrorCode.NoError)}, ErrorCode.NoError);
            
            _kafkaClientEventHandler.HandleOffsetCommitted(offsets);
            
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new { Message="Offsets committed.", Offsets = offsets.Offsets.Select(p=> $"{{topic={p.Topic}}} - {{partition={p.Partition.Value}}} - {{offset:{p.Offset.Value}}}").ToList()});
        }
    }
}