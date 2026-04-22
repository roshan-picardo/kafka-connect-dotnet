using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Connectors;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Builders
{
    public class KafkaClientEventHandlerTests
    {
        private readonly global::Kafka.Connect.Plugin.Logging.ILogger<KafkaClientEventHandler> _logger;
        private readonly IExecutionContext _executionContext;
        private readonly KafkaClientEventHandler _kafkaClientEventHandler;

        public KafkaClientEventHandlerTests()
        {
            _logger = Substitute.For<global::Kafka.Connect.Plugin.Logging.ILogger<KafkaClientEventHandler>>();
            _executionContext = Substitute.For<IExecutionContext>();

            _kafkaClientEventHandler = new KafkaClientEventHandler(_logger, _executionContext);
        }
        
        [Theory]
        [MemberData(nameof(GetErrors))]
        public void HandleError_ReturningExpectedLogs(LogLevel level, Error error)
        {
            _kafkaClientEventHandler.HandleError(error);
            switch (level)
            {
                case LogLevel.Critical: _logger.Received().Critical(error.Reason, Arg.Any<object>()); break;
                case LogLevel.Error: _logger.Received().Error(error.Reason, Arg.Any<object>()); break;
                case LogLevel.Warning: _logger.Received().Warning(error.Reason, Arg.Any<object>()); break;
                case LogLevel.Information: _logger.Received().Info(error.Reason, Arg.Any<object>()); break;
                case LogLevel.Debug: _logger.Received().Debug(error.Reason, Arg.Any<object>()); break;
                case LogLevel.Trace: _logger.Received().Trace(error.Reason, Arg.Any<object>()); break;
                case LogLevel.None: _logger.Received().None(error.Reason, Arg.Any<object>()); break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(level), level, null);
            }
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
            
            switch (logLevel)
            {
                case LogLevel.Critical: _logger.Received().Critical(log.Message, Arg.Any<object>()); break;
                case LogLevel.Error: _logger.Received().Error(log.Message, Arg.Any<object>()); break;
                case LogLevel.Warning: _logger.Received().Warning(log.Message, Arg.Any<object>()); break;
                case LogLevel.Information: _logger.Received().Info(log.Message, Arg.Any<object>()); break;
                case LogLevel.Debug: _logger.Received().Debug(log.Message, Arg.Any<object>()); break;
                case LogLevel.Trace: _logger.Received().Trace(log.Message, Arg.Any<object>()); break;
                case LogLevel.None: _logger.Received().None(log.Message, Arg.Any<object>()); break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(log), logLevel, null);
            }
        }

        [Theory]
        [InlineData(SyslogLevel.Notice)]
        [InlineData(SyslogLevel.Warning)]
        public void HandleLogMessage_WhenConsumerPropertyMessage_LogsTrace(SyslogLevel level)
        {
            const string message = "rdkafka#producer-1 Configuration property group.id is a consumer property and will be ignored by this producer instance";
            var log = new LogMessage("Test", level, "tests", message);

            _kafkaClientEventHandler.HandleLogMessage(log);

            _logger.Received(1).Trace(log.Message, Arg.Any<object>());
            _logger.DidNotReceive().Warning(log.Message, Arg.Any<object>());
        }
        
        [Fact]
        public void HandleStatistics_LogsStatisticsDebugMessage()
        {
            const string stats = "unit test stats";

            _kafkaClientEventHandler.HandleStatistics(stats);

            _logger.Received(1).Debug("Statistics", Arg.Any<object>());
        }
        
        [Fact]
        public void HandlePartitionAssigned_WhenNoPartitionsSet()
        {
            _kafkaClientEventHandler.HandlePartitionAssigned("connector", 1, new List<TopicPartition>());

            _logger.Received().Trace("No partitions assigned.");
            _executionContext.DidNotReceive().AssignPartitions(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<List<TopicPartition>>());
        }

        [Fact]
        public void HandlePartitionAssigned_WhenNullPartitions_LogsTrace()
        {
            _kafkaClientEventHandler.HandlePartitionAssigned("connector", 1, null);

            _logger.Received().Trace("No partitions assigned.");
            _executionContext.DidNotReceive().AssignPartitions(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<List<TopicPartition>>());
        }
        
        
        [Fact]
        public void HandlePartitionAssigned_InvokesOnPartitionAssigned()
        {
            var partitions = new List<TopicPartition> {new("test-one", 0), new("test-one", 1), new("test-2", 0)};
            
            _kafkaClientEventHandler.HandlePartitionAssigned("connector", 1, partitions);
            
            _logger.Received().Debug ("Assigned partitions.", Arg.Any<object>());
            _executionContext.Received().AssignPartitions("connector", 1, partitions);
        }
        
        [Fact]
        public void HandlePartitionRevoked_WhenNoPartitionsSet()
        {
            _kafkaClientEventHandler.HandlePartitionRevoked("connector", 1, new List<TopicPartitionOffset>());

            _logger.Received().Trace("No partitions revoked.");
            _executionContext.DidNotReceive().RevokePartitions(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<List<TopicPartition>>());
        }

        [Fact]
        public void HandlePartitionRevoked_WhenNullOffsets_LogsTrace()
        {
            _kafkaClientEventHandler.HandlePartitionRevoked("connector", 1, null);

            _logger.Received().Trace("No partitions revoked.");
            _executionContext.DidNotReceive().RevokePartitions(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<List<TopicPartition>>());
        }
        
        [Fact]
        public void HandlePartitionRevoked_InvokesOnPartitionRevoked()
        {
            var offsets = new List<TopicPartitionOffset> {new("test-one", 0, 1122), new("test-one", 1, 98192), new("test-2", 0, 0)};
            _kafkaClientEventHandler.HandlePartitionRevoked("connector", 1, offsets);

            _executionContext.Received().RevokePartitions("connector", 1, Arg.Any<IEnumerable<TopicPartition>>());
            _logger.Received().Debug("Revoked partitions.", Arg.Any<object>());
            
        }
        
        [Fact]
        public void Build_OffsetsCommittedHandlerReturningExpectedLogs_WhenNoOffsets()
        {
            _kafkaClientEventHandler.HandleOffsetCommitted(new CommittedOffsets(new List<TopicPartitionOffsetError>(), ErrorCode.NoError));
            
            _logger.Received().Trace("No offsets committed.");
        }
        
        [Fact]
        public void Build_OffsetsCommittedHandlerReturningExpectedLogs_WhenCommitErrored()
        {
            var error = new Error(ErrorCode.OffsetOutOfRange, "commit failed");
            _kafkaClientEventHandler.HandleOffsetCommitted(new CommittedOffsets(new List<TopicPartitionOffsetError>(), error));

            _logger.Received().Warning("Error committing offsets.", Arg.Any<object>());
        }
        
        [Fact]
        public void Build_OffsetsCommittedHandlerReturningExpectedLogs_WhenCommitErroredWithOffsets()
        {
            var error = new Error(ErrorCode.OffsetOutOfRange, "commit failed");
            _kafkaClientEventHandler.HandleOffsetCommitted(new CommittedOffsets(new List<TopicPartitionOffsetError> { new("topic", 1, 1000, error) }, error));
            
            _logger.Received().Warning( "Error committing offsets.",Arg.Any<object>());
        }
        
        [Fact]
        public void Build_PartitionRevokedHandlerReturningExpectedLogs_WhenOffsetsCommitted()
        {
            var offsets =
                new CommittedOffsets(new List<TopicPartitionOffsetError> {new("topic", 0, 1123, ErrorCode.NoError), new("topic", 5, 1123, ErrorCode.NoError)}, ErrorCode.NoError);
            
            _kafkaClientEventHandler.HandleOffsetCommitted(offsets);
            
            _logger.Received().Debug("Offsets committed.", Arg.Any<object>());
        }
        
        
    }
}