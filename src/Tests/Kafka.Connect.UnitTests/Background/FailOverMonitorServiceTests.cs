using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Background;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace UnitTests.Kafka.Connect.Background
{
    public class FailOverMonitorServiceTests
    {
        private readonly ILogger<FailOverMonitorService> _logger;
        private readonly IKafkaClientBuilder _kafkaClientBuilder;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        private FailOverMonitorService _failOverMonitorService;
        private readonly IAdminClient _adminClient;
        private readonly ITokenHandler _tokenHandler;
        private readonly IConnector _connector;
        private readonly IConfigurationProvider _configProvider;
        private readonly IExecutionContext _executionContext;

        public FailOverMonitorServiceTests()
        {
            _logger = Substitute.For<ILogger<FailOverMonitorService>>();
            _serviceScopeFactory = Substitute.For<IServiceScopeFactory>();
            _kafkaClientBuilder = Substitute.For<IKafkaClientBuilder>();
            _adminClient = Substitute.For<IAdminClient>();
            _tokenHandler = Substitute.For<ITokenHandler>();
            _connector = Substitute.For<IConnector>();
            _configProvider = Substitute.For<IConfigurationProvider>();
            _executionContext = Substitute.For<IExecutionContext>();
        }

        [Fact]
        public void ExecuteAsync_ServiceNotEnabled()
        {
            _configProvider.GetFailOverConfig().Returns(new FailOverConfig {Disabled = true});
            _failOverMonitorService = GetFailOverMonitorService();
            _failOverMonitorService.StartAsync(new CancellationToken());
            _logger.Received(1).Debug("Fail over monitoring service is not enabled...");
        }

        [Fact]
        public void ServiceEnabledButCancellationRequested_StopsService()
        {
            _configProvider.GetFailOverConfig().Returns(new FailOverConfig {Disabled = false, InitialDelayMs = 1, PeriodicDelayMs = 1});
            _configProvider.GetAllConnectorConfigs().Returns(new List<ConnectorConfig>());
            _failOverMonitorService = GetFailOverMonitorService();
            _failOverMonitorService.StartAsync(GetCancellationToken(1));
            while (!_failOverMonitorService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            
            _kafkaClientBuilder.Received().GetAdminClient();
            _logger.Received().Debug("Starting fail over monitoring service...");
            _logger.Received().Debug("Stopping fail over monitoring service...");
        }

        [Theory]
        [InlineData(new [] {"one", "two"}, "three", 3)]
        [InlineData(new [] {"one", "two"}, null, 2)]
        [InlineData(null, "one", 1)]
        public void ExecuteAsync_MakeSureGetMetadataCalledPerTopic(string[] topics, string topic, int expected)
        {
            _configProvider.GetFailOverConfig().Returns(new FailOverConfig {FailureThreshold = 3, InitialDelayMs = 1, PeriodicDelayMs = 1, RestartDelayMs = 1});
            _configProvider.GetAllConnectorConfigs().Returns(new List<ConnectorConfig>
            {
                new()
                {
                    Name = "unit-test-fail-over-enabled",
                    Disabled = false,
                    Topic = topic,
                    Topics = topics?.ToList()
                }
            });
            
            _adminClient.GetMetadata(Arg.Any<string>(), Arg.Any<TimeSpan>()).Returns(null as Metadata);
            _failOverMonitorService = GetFailOverMonitorService();
            
            _failOverMonitorService.StartAsync(GetCancellationToken(1));
            
            while (!_failOverMonitorService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            _adminClient.Received(expected).GetMetadata(Arg.Any<string>(), Arg.Any<TimeSpan>());
        }

        [Fact]
        public void ExecuteAsync_BrokerFailureForAllConnectorsAndWorkerRestart()
        {
            var failOverConfig = new FailOverConfig {InitialDelayMs = 1, FailureThreshold = 3, PeriodicDelayMs = 1, RestartDelayMs = 1};
            _configProvider.GetFailOverConfig().Returns(failOverConfig);
            _configProvider.GetAllConnectorConfigs().Returns(new List<ConnectorConfig>
            {
                new()
                {
                    Name = "unit-test-fail-over-enabled-a",
                    Disabled = false,
                    Topics = new List<string>{ "TopicFailingA" }
                },
                new()
                {
                    Name = "unit-test-fail-over-enabled-b",
                    Disabled = false,
                    Topics = new List<string>{ "TopicFailingB" }
                }
            });

            _adminClient.GetMetadata(Arg.Any<string>(), Arg.Any<TimeSpan>()).Returns(args =>
                new Metadata(new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                        {new(args[0] as string, new List<PartitionMetadata>(), ErrorCode.BrokerNotAvailable)}, 0, ""));
            
            _failOverMonitorService = GetFailOverMonitorService();
            _failOverMonitorService.StartAsync(GetCancellationToken(3));
            
            while (!_failOverMonitorService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            _adminClient.Received(3).GetMetadata("TopicFailingA", Arg.Any<TimeSpan>());
            _adminClient.Received(3).GetMetadata("TopicFailingB", Arg.Any<TimeSpan>());
            for (var i = failOverConfig.FailureThreshold; i == 0; i--)
            {
                _logger.Received().Trace("Broker failure detected.", new {Connector = "unit-test-fail-over-enabled-a", Threshold = i });
                _logger.Received().Trace( "Broker failure detected.", new{Connector = "unit-test-fail-over-enabled-b", Threshold = i });
            }

            _executionContext.Received().Restart(Arg.Any<int>());
        }
        
        [Fact]
        public void ExecuteAsync_BrokerFailureForOneConnectorAndConnectorRestart()
        {
            var failOverConfig = new FailOverConfig {FailureThreshold = 3, InitialDelayMs = 1, PeriodicDelayMs = 1, RestartDelayMs = 1};
            _configProvider.GetFailOverConfig().Returns(failOverConfig);
            _configProvider.GetAllConnectorConfigs().Returns(new List<ConnectorConfig>
            {
                new()
                {
                    Name = "unit-test-fail-over-enabled-a",
                    Disabled = false,
                    Topics = new List<string>{ "TopicFailing" }
                },
                new()
                {
                    Name = "unit-test-fail-over-enabled-b",
                    Disabled = false,
                    Topics = new List<string>{ "TopicPassing" }
                }
            });
            

            _adminClient.GetMetadata("TopicFailing", Arg.Any<TimeSpan>()).Returns(args =>
                new Metadata(new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                        {new(args[0] as string, new List<PartitionMetadata>(), ErrorCode.BrokerNotAvailable)}, 0, ""));
            _adminClient.GetMetadata("TopicPassing", Arg.Any<TimeSpan>()).Returns(args =>
                new Metadata(new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                        {new(args[0] as string, new List<PartitionMetadata>(), ErrorCode.NoError)}, 0, ""));
            _executionContext.GetConnector(Arg.Any<string>()).Returns(_connector);
            
            _failOverMonitorService = GetFailOverMonitorService();
            
            _failOverMonitorService.StartAsync(GetCancellationToken(3));
            
            while (!_failOverMonitorService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            _adminClient.Received(3).GetMetadata("TopicPassing", Arg.Any<TimeSpan>());
            _adminClient.Received(3).GetMetadata("TopicFailing", Arg.Any<TimeSpan>());
            for (var i = failOverConfig.FailureThreshold; i == 0; i--)
            {
                _logger.Received().Trace("Broker failure detected.", new {Connector = "unit-test-fail-over-enabled-a", Threshold = 5 });
                _logger.Received().Trace( "Broker failure detected.", new{Connector = "unit-test-fail-over-enabled-b", Threshold = i });
            }

            _executionContext.Received(1).Restart(Arg.Any<int>(), "unit-test-fail-over-enabled-a");
        }
        
        [Theory]
        [MemberData(nameof(GetMetadataScenariosTestData))]
        public void ExecuteAsync_TopicMetadataScenarios(string topic, Metadata metadata, int expected)
        {
            var failOverConfig = new FailOverConfig {FailureThreshold = 3, InitialDelayMs = 1, PeriodicDelayMs = 1, RestartDelayMs = 1};
            _configProvider.GetFailOverConfig().Returns(failOverConfig);
            _configProvider.GetAllConnectorConfigs().Returns(new List<ConnectorConfig>
            {
                new()
                {
                    Name = "unit-test-fail-over-enabled",
                    Disabled = false,
                    Topic = topic
                }
            });
            
            _adminClient.GetMetadata(Arg.Any<string>(), Arg.Any<TimeSpan>())
                .Returns(metadata);
            
            _failOverMonitorService = GetFailOverMonitorService();
            _failOverMonitorService.StartAsync(GetCancellationToken(1));
            while (!_failOverMonitorService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            _logger.Received(expected).Trace("Broker failure detected.", Arg.Any<object>());
        }

        [Fact]
        public void ExecuteAsync_ExamineFailOverSingleConnectorThresholdResets()
        {
            var failOverConfig = new FailOverConfig {FailureThreshold = 3, InitialDelayMs = 1, PeriodicDelayMs = 1, RestartDelayMs = 1};
            _configProvider.GetFailOverConfig().Returns(failOverConfig);
            _configProvider.GetAllConnectorConfigs().Returns(new List<ConnectorConfig>
            {
                new()
                {
                    Name = "unit-test-fail-over-enabled",
                    Disabled = false,
                    Topic = "test-topic"
                }
            });
            
            _adminClient.GetMetadata(Arg.Any<string>(), Arg.Any<TimeSpan>())
                .Returns(
                    _ => new Metadata(new List<BrokerMetadata>(), new List<TopicMetadata> { new("test-topic", new List<PartitionMetadata>(), ErrorCode.BrokerNotAvailable)}, 0, ""),
                    _ => new Metadata(new List<BrokerMetadata>(), new List<TopicMetadata> { new("test-topic", new List<PartitionMetadata>(), ErrorCode.BrokerNotAvailable)}, 0, ""),
                    _ => new Metadata(new List<BrokerMetadata>(), new List<TopicMetadata> {new("test-topic", new List<PartitionMetadata>(), ErrorCode.NoError)}, 0, ""),
                    _ => new Metadata(new List<BrokerMetadata>(), new List<TopicMetadata> { new("test-topic", new List<PartitionMetadata>(), ErrorCode.BrokerNotAvailable)}, 0, ""));
            _failOverMonitorService = GetFailOverMonitorService();

            _failOverMonitorService.StartAsync(GetCancellationToken(4));

            while (!_failOverMonitorService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }

            _adminClient.Received(4).GetMetadata("test-topic", Arg.Any<TimeSpan>());
            _logger.Received(3).Trace( "Broker failure detected.", Arg.Any<object>());
        }
        
        [Fact]
        public void ExecuteAsync_OneConnectorMetadataThrowsException()
        {
            var failOverConfig = new FailOverConfig {FailureThreshold = 3, InitialDelayMs = 1, PeriodicDelayMs = 1, RestartDelayMs = 1};
            _configProvider.GetFailOverConfig().Returns(failOverConfig);
            _configProvider.GetAllConnectorConfigs().Returns(new List<ConnectorConfig>
            {
                new()
                {
                    Name = "unit-test-fail-over-exception",
                    Disabled = false,
                    Topics = new List<string>{ "topic-exception" }
                },
                new()
                {
                    Name = "unit-test-fail-over-general-Failure",
                    Disabled = false,
                    Topics = new List<string>{ "topic-error" }
                },
                new()
                {
                    Name = "unit-test-fail-over-passing",
                    Disabled = false,
                    Topics = new List<string>{ "topic-pass" }
                }
            });

            _adminClient.GetMetadata("topic-exception", Arg.Any<TimeSpan>()).Throws(new Exception("UnitTestFailure"));
            _adminClient.GetMetadata("topic-error", Arg.Any<TimeSpan>()).Returns(args =>
                new Metadata(new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                        {new(args[0] as string, new List<PartitionMetadata>(), ErrorCode.BrokerNotAvailable)}, 0, ""));
            _adminClient.GetMetadata("topic-pass", Arg.Any<TimeSpan>()).Returns(args =>
                new Metadata(new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                        {new(args[0] as string, new List<PartitionMetadata>(), ErrorCode.NoError)}, 0, ""));
            
            _failOverMonitorService = GetFailOverMonitorService();
            _failOverMonitorService.StartAsync(GetCancellationToken(1));
            
            while (!_failOverMonitorService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            _adminClient.Received(1).GetMetadata("topic-exception", Arg.Any<TimeSpan>());
            _adminClient.Received(1).GetMetadata("topic-error", Arg.Any<TimeSpan>());
            _adminClient.Received(1).GetMetadata("topic-pass", Arg.Any<TimeSpan>());
            
            _logger.Received(2).Trace("Broker failure detected.",Arg.Any<object>());
            //_logger.Received(0).Trace("Broker failure detected.", new {Connector = "unit-test-fail-over-passing", Threshold = 2 });
            //_logger.Received(1).Trace("Broker failure detected.",new {Connector = "unit-test-fail-over-exception", Threshold = 2 });
            _logger.Received(1).Error("Unhandled error while reading metadata.",Arg.Any<object>(), Arg.Any<Exception>());
        }
        
        [Fact]
        public void ExecuteAsync_RestartWorkerWhenMetadataReturnsErrorAndExceptions()
        {
            var failOverConfig = new FailOverConfig {FailureThreshold = 3, InitialDelayMs = 1, PeriodicDelayMs = 1, RestartDelayMs = 1};
            _configProvider.GetFailOverConfig().Returns(failOverConfig);
            _configProvider.GetAllConnectorConfigs().Returns(new List<ConnectorConfig>
            {
                new()
                {
                    Name = "unit-test-fail-over-exception",
                    Disabled = false,
                    Topics = new List<string>{ "topic-exception" }
                }
            });

            _adminClient.GetMetadata("topic-exception", Arg.Any<TimeSpan>()).Returns(args =>
                new Metadata(new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                        {new(args[0] as string, new List<PartitionMetadata>(), ErrorCode.BrokerNotAvailable)}, 0, ""));
            _adminClient.GetMetadata("topic-exception", Arg.Any<TimeSpan>()).Throws(new Exception("UnitTestFailure"));

            _failOverMonitorService = GetFailOverMonitorService();
            _failOverMonitorService.StartAsync(GetCancellationToken(3));
            
            while (!_failOverMonitorService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            _adminClient.Received(3).GetMetadata("topic-exception", Arg.Any<TimeSpan>());
            for (var i = failOverConfig.FailureThreshold; i == 0; i--)
            {
                _logger.Received(1).Trace("Broker failure detected.",new{ Connector = "unit-test-fail-over-exception", Threshold= i });
                _logger.Received(1).Error("Unhandled error while reading metadata.",new {Connector = "unit-test-fail-over-exception",Threshold=i}, Arg.Any<Exception>());
            }

            _executionContext.Received().Restart(Arg.Any<int>());
        }
        
        [Fact]
        public void ExecuteAsync_ThrowsExceptionAtRestart()
        {
            var failOverConfig = new FailOverConfig {FailureThreshold = 1, InitialDelayMs = 1, PeriodicDelayMs = 1, RestartDelayMs = 1};
            _configProvider.GetFailOverConfig().Returns(failOverConfig);
            _configProvider.GetAllConnectorConfigs().Returns(new List<ConnectorConfig>
            {
                new()
                {
                    Name = "unit-test-fail-over",
                    Disabled = false,
                    Topics = new List<string> {"topic-test"}
                }
            });
            
            _adminClient.GetMetadata("topic-test", Arg.Any<TimeSpan>()).Throws(new Exception("UnitTestFailure"));
            _executionContext.Restart(Arg.Any<int>()).Throws<Exception>();
            
            _failOverMonitorService = GetFailOverMonitorService();
            _failOverMonitorService.StartAsync(GetCancellationToken(1));
            
            while (!_failOverMonitorService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            
            _adminClient.Received(1).GetMetadata("topic-test", Arg.Any<TimeSpan>());
            _executionContext.Received(1).Restart(Arg.Any<int>());
            _logger.Received(1).Trace("Broker failure detected.",Arg.Any<object>());
            _logger.Received(1).Error( "Fail over monitoring service reported errors / hasn't started." , Arg.Any<Exception>());
            _logger.Received(1).Debug("Stopping fail over monitoring service...");
        }
        
        [Theory]
        [InlineData(typeof(TaskCanceledException))]
        [InlineData(typeof(OperationCanceledException))]
        public void ExecuteAsync_ThrowsTaskCanceledExceptionAtRestart(Type exType)
        {
            var failOverConfig = new FailOverConfig {FailureThreshold = 1, InitialDelayMs = 1, PeriodicDelayMs = 1, RestartDelayMs = 1};
            _configProvider.GetFailOverConfig().Returns(failOverConfig);
            _configProvider.GetAllConnectorConfigs().Returns(new List<ConnectorConfig>
            {
                new()
                {
                    Name = "unit-test-fail-over",
                    Disabled = false,
                    Topics = new List<string> {"topic-test"}
                }
            });
            _adminClient.GetMetadata("topic-test", Arg.Any<TimeSpan>()).Throws(new Exception("UnitTestFailure"));
            _executionContext.Restart(Arg.Any<int>()).Throws(Activator.CreateInstance(exType, "Token Cancelled.") as Exception);

            _failOverMonitorService = GetFailOverMonitorService();
            _failOverMonitorService.StartAsync(GetCancellationToken(1));
            
            while (!_failOverMonitorService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            
            _adminClient.Received(1).GetMetadata("topic-test", Arg.Any<TimeSpan>());
            _executionContext.Received(1).Restart(Arg.Any<int>());
            _logger.Received(1).Trace( "Broker failure detected.", Arg.Any<object>());
            _logger.Received(1).Trace( "Task has been cancelled. Fail over service will be terminated." );
            _logger.Received(1).Debug("Stopping fail over monitoring service..." );
        }

        public static IEnumerable<object[]> GetMetadataScenariosTestData
        {
            get
            {
                yield return new object[] { "test-topic", null, 0 };
                yield return new object[] { "test-topic", new Metadata(new List<BrokerMetadata>(), new List<TopicMetadata>(), 0, ""), 0 };
                yield return new object[] { "test-topic", new Metadata(new List<BrokerMetadata>(), new List<TopicMetadata> {new TopicMetadata("test-topic", new List<PartitionMetadata>(), ErrorCode.NoError)}, 0, ""), 0};
                yield return new object[] { "some-other-topic", new Metadata(new List<BrokerMetadata>(), new List<TopicMetadata> {new TopicMetadata("test-topic", new List<PartitionMetadata>(), ErrorCode.NoError)}, 0, ""), 0};
                yield return new object[] { "some-other-topic", new Metadata(new List<BrokerMetadata>(), new List<TopicMetadata>() {new TopicMetadata("test-topic", new List<PartitionMetadata>(), ErrorCode.BrokerNotAvailable)},0, ""), 0};
                yield return new object[] { "test-topic", new Metadata(new List<BrokerMetadata>(), new List<TopicMetadata>() {new TopicMetadata("test-topic", new List<PartitionMetadata>(), ErrorCode.BrokerNotAvailable)},0, ""), 1};
            }
        }

        private FailOverMonitorService GetFailOverMonitorService()
        {
            _serviceScopeFactory.CreateScope().ServiceProvider.GetService<IKafkaClientBuilder>()
                .Returns(_kafkaClientBuilder);
            _kafkaClientBuilder.GetAdminClient().Returns(_adminClient);
            return new FailOverMonitorService(_logger, _executionContext, _serviceScopeFactory, _tokenHandler, _configProvider);
        }
        
        private CancellationToken GetCancellationToken(int loop)
        {
            var cts = new CancellationTokenSource();

            _tokenHandler.When(k => k.DoNothing()).Do(_ =>
            {
                if (--loop == 0) cts.Cancel();
            });
            return cts.Token;
        }
        
    }
}