using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Connect.Builders;
using Kafka.Connect.Config;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Kafka.Connect.Tests.Connectors
{
    public class ConnectDeadLetterTests
    {
        private readonly ILogger<ConnectDeadLetter> _logger;
        private readonly IKafkaClientBuilder _kafkaClientBuilder;
        private readonly ConnectDeadLetter _connectDeadLetter;
        private readonly IAdminClient _adminClient;
        private readonly IProducer<byte[], byte[]> _producer;

        public ConnectDeadLetterTests()
        {
            _logger = Substitute.For<ILogger<ConnectDeadLetter>>();
            _kafkaClientBuilder = Substitute.For<IKafkaClientBuilder>();
            _adminClient = Substitute.For<IAdminClient>();
            _producer = Substitute.For<IProducer<byte[], byte[]>>();
            _connectDeadLetter = new ConnectDeadLetter(_logger, _kafkaClientBuilder);
        }

        #region CreateTopic

        [Theory]
        [MemberData(nameof(ConfigsWhenNoTopicCreated))]
        public async Task CreateTopic_DoNotCreateTopic_Tests(ErrorConfig errorConfig)
        {
            await _connectDeadLetter.CreateTopic(new ConnectorConfig() {Errors = errorConfig});

            _logger.DidNotReceive().Log(LogLevel.Information, 0, Arg.Any<string>(), null,
                Arg.Any<Func<string, Exception, string>>());
        }

        [Fact]
        public async Task CreateTopic_TopicCreated_Tests()
        {
            var config = new ConnectorConfig()
            {
                Errors = new ErrorConfig
                {
                    Tolerance = ErrorTolerance.All,
                    DeadLetter = new DeadLetterConfig
                    {
                        Create = true,
                        Topic = "dead-letter-topic"
                    }
                }
            };

            _kafkaClientBuilder.GetAdminClient(config).Returns(_adminClient);

            await _connectDeadLetter.CreateTopic(config);

            _kafkaClientBuilder.Received().GetAdminClient(config);
            await _adminClient.Received().CreateTopicsAsync(Arg.Any<IEnumerable<TopicSpecification>>());
            _logger.Received().LogInformation(
                $"Dead Letter topic : {config.Errors.DeadLetter.Topic} has been created successfully!");
        }

        [Theory]
        [MemberData(nameof(CreateTopicExceptionReport))]
        public async Task CreateTopic_ThrowsCreateTopicException(CreateTopicReport topicReport, bool isError)
        {
            var config = new ConnectorConfig()
            {
                Errors = new ErrorConfig
                {
                    Tolerance = ErrorTolerance.All,
                    DeadLetter = new DeadLetterConfig
                    {
                        Create = true,
                        Topic = "dead-letter-topic"
                    }
                }
            };

            _kafkaClientBuilder.GetAdminClient(config).Returns(_adminClient);
            var cte = new CreateTopicsException(new List<CreateTopicReport>() {topicReport});
            _adminClient.CreateTopicsAsync(Arg.Any<IEnumerable<TopicSpecification>>()).Throws(cte);

            await _connectDeadLetter.CreateTopic(config);

            _kafkaClientBuilder.Received().GetAdminClient(config);
            await _adminClient.Received().CreateTopicsAsync(Arg.Any<IEnumerable<TopicSpecification>>());
            if (isError)
            {
                _logger.DidNotReceive().LogInformation(topicReport.Error.Reason);
                // _logger.Received().Log(LogLevel.Error, 0, Arg.Any<object>(), Arg.Any<Exception>(),
                //     Arg.Any<Func<object, Exception, string>>());
            }
            else
            {
                _logger.Received().LogInformation(topicReport.Error.Reason);
            }
        }

        public static IEnumerable<object[]> ConfigsWhenNoTopicCreated
        {
            get
            {
                yield return new object[] {null};
                yield return new object[] {new ErrorConfig()};
                yield return new object[] {new ErrorConfig {DeadLetter = new DeadLetterConfig()}};
                yield return new object[] {new ErrorConfig {DeadLetter = new DeadLetterConfig()}};
                yield return new object[]
                    {new ErrorConfig {DeadLetter = new DeadLetterConfig(), Tolerance = ErrorTolerance.All}};
                yield return new object[]
                    {new ErrorConfig {DeadLetter = new DeadLetterConfig(), Tolerance = ErrorTolerance.None}};
                yield return new object[]
                {
                    new ErrorConfig
                        {DeadLetter = new DeadLetterConfig {Create = true}, Tolerance = ErrorTolerance.None}
                };
                yield return new object[]
                {
                    new ErrorConfig
                        {DeadLetter = new DeadLetterConfig {Create = true, Topic = ""}, Tolerance = ErrorTolerance.None}
                };
            }
        }

        public static IEnumerable<object[]> CreateTopicExceptionReport
        {
            get
            {
                yield return new object[]
                    {new CreateTopicReport {Topic = "dead-letter-topic", Error = ErrorCode.Unknown}, true};
                yield return new object[]
                    {new CreateTopicReport {Topic = "unknown", Error = ErrorCode.TopicAlreadyExists}, true};
                yield return new object[]
                    {new CreateTopicReport {Topic = "dead-letter-topic", Error = ErrorCode.TopicAlreadyExists}, false};
            }
        }

        #endregion

        #region Send

        [Theory]
        [InlineData(null, ErrorTolerance.None)]
        [InlineData("", ErrorTolerance.None)]
        [InlineData("dead-letter-topic", ErrorTolerance.None)]
        [InlineData("dead-letter-topic", ErrorTolerance.All)] // no sink records
        public async Task Send_IgnoreToForward_Tests(string topic, ErrorTolerance tolerance)
        {
            var config = new ConnectorConfig()
            {
                Errors = new ErrorConfig
                {
                    Tolerance = tolerance,
                    DeadLetter = new DeadLetterConfig
                    {
                        Create = true,
                        Topic = topic
                    }
                }
            };

            await _connectDeadLetter.Send(config, null, null);

            _kafkaClientBuilder.DidNotReceive().GetProducer(config);
            await _producer.DidNotReceive().ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>());
        }

        [Fact]
        public async Task Send_ToDeadLetter_Tests()
        {
            var config = new ConnectorConfig()
            {
                Errors = new ErrorConfig
                {
                    Tolerance = ErrorTolerance.All,
                    DeadLetter = new DeadLetterConfig
                    {
                        Create = true,
                        Topic = "dead-letter-topic"
                    }
                }
            };
            var sinkRecord =
                new SinkRecord(new ConsumeResult<byte[], byte[]>()
                {
                    Message = new Message<byte[], byte[]>()
                    {
                        Headers = new Headers()
                    }
                });

            _kafkaClientBuilder.GetProducer(config).Returns(_producer);
            _producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>())
                .Returns(new DeliveryResult<byte[], byte[]>
                {
                    Partition = new Partition(),
                    Offset = new Offset()
                });

            await _connectDeadLetter.Send(config, new[] {sinkRecord}, new Exception());

            _kafkaClientBuilder.Received().GetProducer(config);
            Assert.True(sinkRecord.Consumed.Message.Headers.TryGetLastBytes("_errorContext", out _));
            Assert.True(sinkRecord.Consumed.Message.Headers.TryGetLastBytes("_sourceContext", out _));

            await _producer.Received().ProduceAsync("dead-letter-topic", Arg.Any<Message<byte[], byte[]>>());
        }

        #endregion

    }
}