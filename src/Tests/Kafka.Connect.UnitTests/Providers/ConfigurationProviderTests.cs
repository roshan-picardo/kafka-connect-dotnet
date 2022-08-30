using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Providers
{
    public class ConfigurationProviderTests
    {

        [Theory]
        [MemberData(nameof(FailOverConfigTests))]
        public void GetFailOverConfig_Tests(FailOverConfig input, FailOverConfig expected)
        {
            var actual = GetProvider(new WorkerConfig() {FailOver = input}).GetFailOverConfig();

            Assert.NotNull(actual);
            Assert.Equal(expected.Disabled, actual.Disabled);
            Assert.Equal(expected.InitialDelayMs, actual.InitialDelayMs);
            Assert.Equal(expected.PeriodicDelayMs, actual.PeriodicDelayMs);
            Assert.Equal(expected.FailureThreshold, actual.FailureThreshold);
            Assert.Equal(expected.RestartDelayMs, actual.RestartDelayMs);
        }

        [Theory]
        [MemberData(nameof(HealthCheckConfigTests))]
        public void GetHealthCheckConfig_Tests(HealthCheckConfig input, HealthCheckConfig expected)
        {
            var actual = GetProvider(new WorkerConfig() {HealthCheck = input}).GetHealthCheckConfig();

            Assert.NotNull(actual);
            Assert.Equal(expected.Disabled, actual.Disabled);
            Assert.Equal(expected.InitialDelayMs, actual.InitialDelayMs);
            Assert.Equal(expected.PeriodicDelayMs, actual.PeriodicDelayMs);
        }

        [Theory]
        [MemberData(nameof(RestartsConfigTests))]
        public void GetRestartsConfig_Tests(RestartsConfig input, RestartsConfig expected)
        {
            var actual = GetProvider(new WorkerConfig() {Restarts = input}).GetRestartsConfig();

            Assert.NotNull(actual);
            Assert.Equal(expected.EnabledFor, actual.EnabledFor);
            Assert.Equal(expected.RetryWaitTimeMs, actual.RetryWaitTimeMs);
            Assert.Equal(expected.PeriodicDelayMs, actual.PeriodicDelayMs);
        }

        [Theory]
        [InlineData(null, false, "GET-MACHINE-NAME")]
        [InlineData("from-env-var", true, "from-env-var")]
        [InlineData("from-config-var", false, "from-config-var")]
        public void GetWorkerName_Tests(string configValue, bool isEnvVar, string expected)
        {
            expected = expected == "GET-MACHINE-NAME" ? Environment.MachineName : expected;
            var workerConfig = new WorkerConfig() {Name = isEnvVar ? null : configValue};
            if (isEnvVar)
            {
                Environment.SetEnvironmentVariable("WORKER_HOST", configValue);
            }

            var actual = GetProvider(workerConfig).GetWorkerName();

            Assert.Equal(expected, actual);
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public void GetConnectorConfig_ThrowsException(bool isNull, bool isEmpty)
        {
            Assert.Throws<ArgumentException>(() => GetProvider(new WorkerConfig()
            {
                Connectors = isNull ? null :
                    isEmpty ? new Dictionary<string, ConnectorConfig>() : new Dictionary<string, ConnectorConfig>()
                        {{"some-unknown-connector", new ConnectorConfig()}}
            }).GetConnectorConfig("expected-connector"));
        }

        [Theory]
        [InlineData("expected-connector", null, "expected-connector")]
        [InlineData("expected-connector", "", "expected-connector")]
        [InlineData("expected-connector-something", "expected-connector", "expected-connector")]
        public void GetConnectorConfig_ReturnsConnectorConfig(string key, string name, string expected)
        {
            var actual = GetProvider(new WorkerConfig()
                {
                    Connectors = new Dictionary<string, ConnectorConfig>() {{key, new ConnectorConfig() {Name = name}}}
                })
                .GetConnectorConfig(expected);
            Assert.NotNull(actual);
            Assert.Equal(expected, actual.Name);
        }

        [Theory]
        [InlineData(null, "GroupId", "ClientId", null, null)]
        [InlineData("expected-connector", null, null, "expected-connector", "expected-connector")]
        [InlineData("expected-connector", "expected-groupId", "expected-clientId", "expected-groupId",
            "expected-clientId")]
        public void GetConsumerConfig_Tests(string connector, string groupId, string clientId, string expectedGroupId,
            string expectedClientId)
        {
            var actual = GetProvider(new WorkerConfig()
            {
                Connectors = new Dictionary<string, ConnectorConfig>()
                    {{"0", new ConnectorConfig() {Name = connector, GroupId = groupId, ClientId = clientId}}}
            }).GetConsumerConfig(connector);

            Assert.NotNull(actual);
            Assert.IsType<WorkerConfig>(actual);
            Assert.Equal(actual.GroupId, expectedGroupId);
            Assert.Equal(actual.ClientId, expectedClientId);
        }

        [Fact]
        public void GetProducerConfig_Tests()
        {
            var actual = GetProvider(new WorkerConfig()
            {
                Connectors = new Dictionary<string, ConnectorConfig>() {{"expected-connector", new ConnectorConfig()}}
            }).GetProducerConfig("expected-connector");

            Assert.NotNull(actual);
            Assert.IsType<ProducerConfig>(actual);
        }

        [Fact]
        public void GetAllConnectorConfigs_ThrowsExceptionWhenNoConnectorsConfigured()
        {
            Assert.Throws<ArgumentException>(() => GetProvider(new WorkerConfig()).GetAllConnectorConfigs());
        }

        [Theory]
        [InlineData(true, 5)]
        [InlineData(false, 3)]
        public void GetAllConnectorConfigs_ReturnsConnectors(bool includeDisabled, int expectedCount)
        {
            var actual = GetProvider(new WorkerConfig
            {
                Connectors = new Dictionary<string, ConnectorConfig>
                {
                    {"Enabled-1", new ConnectorConfig {Disabled = false}},
                    {"Enabled-2", new ConnectorConfig {Disabled = false}},
                    {"Disabled-1", new ConnectorConfig {Disabled = true}},
                    {"Disabled-2", new ConnectorConfig {Disabled = true}},
                    {"Enabled-3", new ConnectorConfig {Disabled = false}}
                }
            }).GetAllConnectorConfigs(includeDisabled);

            Assert.NotNull(actual);
            Assert.Equal(expectedCount, actual.Count);
        }

        [Theory]
        [MemberData(nameof(ErrorsConfigTests))]
        public void GetErrorsConfig_Tests(RetryConfig connectorRetry, RetryConfig workerRetry,
            ErrorsConfig expectedRetry)
        {
            var actual = GetProvider(new WorkerConfig()
            {
                Retries = workerRetry,
                Connectors = new Dictionary<string, ConnectorConfig>()
                {
                    {"connector", new ConnectorConfig() {Retries = connectorRetry}}
                }
            }).GetErrorsConfig("connector");

            Assert.NotNull(actual);
            Assert.Equal(expectedRetry.Topic, actual.Topic);
            Assert.Equal(expectedRetry.Tolerance, actual.Tolerance);
        }

        [Theory]
        [MemberData(nameof(RetriesConfigTests))]
        public void GetRetriesConfig_Tests(RetryConfig connectorRetry, RetryConfig workerRetry,
            RetryConfig expectedRetry)
        {
            var actual = GetProvider(new WorkerConfig
            {
                Retries = workerRetry,
                Connectors = new Dictionary<string, ConnectorConfig>
                {
                    {"connector", new ConnectorConfig {Retries = connectorRetry}}
                }
            }).GetRetriesConfig("connector");

            Assert.NotNull(actual);
            Assert.Equal(expectedRetry.Attempts, actual.Attempts);
            Assert.Equal(expectedRetry.DelayTimeoutMs, actual.DelayTimeoutMs);
        }

        [Theory]
        [MemberData(nameof(EofSignalConfigTests))]
        public void GetEofSignalConfig_Tests(BatchConfig connectorBatch, BatchConfig workerBatch, EofConfig expectedEof)
        {
            var actual = GetProvider(new WorkerConfig
            {
                Batches = workerBatch,
                Connectors = new Dictionary<string, ConnectorConfig>
                {
                    {"connector", new ConnectorConfig {Batches = connectorBatch}}
                }
            }).GetEofSignalConfig("connector");

            Assert.NotNull(actual);
            Assert.Equal(expectedEof.Enabled, actual.Enabled);
            Assert.Equal(expectedEof.Topic, actual.Topic);
        }

        [Theory]
        [MemberData(nameof(BatchConfigTests))]
        public void GetBatchConfig_Tests(bool enablePartitionEof, BatchConfig connectorBatch, BatchConfig workerBatch,
            BatchConfig expectedBatch)
        {
            var actual = GetProvider(new WorkerConfig
            {
                EnablePartitionEof = enablePartitionEof,
                Batches = workerBatch,
                Connectors = new Dictionary<string, ConnectorConfig>
                {
                    {"connector", new ConnectorConfig {Batches = connectorBatch}}
                }
            }).GetBatchConfig("connector");

            Assert.NotNull(actual);
            Assert.Equal(expectedBatch.Size, actual.Size);
            Assert.Equal(expectedBatch.Parallelism, actual.Parallelism);
        }

        [Fact]
        public void GetGroupId_Tests()
        {
            var actual = GetProvider(new WorkerConfig
            {
                Connectors = new Dictionary<string, ConnectorConfig>
                {
                    {"connector-group-id", new ConnectorConfig()}
                }
            }).GetGroupId("connector-group-id");

            Assert.Equal("connector-group-id", actual);
        }

        [Theory]
        [MemberData(nameof(MessageConvertersTests))]
        public void GetMessageConverters_Tests(BatchConfig connectorBatch, BatchConfig workerBatch,
            (string Key, string Value) expectedSerializer)
        {
            var (key, value) = GetProvider(new WorkerConfig
            {
                Batches = workerBatch,
                Connectors = new Dictionary<string, ConnectorConfig>
                {
                    {"connector", new ConnectorConfig {Batches = connectorBatch}}
                }
            }).GetMessageConverters("connector", "test-topic");

            Assert.Equal(expectedSerializer.Key, key);
            Assert.Equal(expectedSerializer.Value, value);
        }

        [Theory]
        [InlineData(null, null, new string[0])]
        [InlineData("", null, new string[0])]
        [InlineData("", new string[0], new string[0])]
        [InlineData("topic-single", null, new[] {"topic-single"})]
        [InlineData("topic-single", new string[0], new[] {"topic-single"})]
        [InlineData("topic-single", new[] {"topic-list-one", "topic-list-two"},
            new[] {"topic-single", "topic-list-one", "topic-list-two"})]
        [InlineData("topic-list-one", new[] {"topic-list-one", "topic-list-two"},
            new[] {"topic-list-one", "topic-list-two"})]
        [InlineData(null, new[] {"topic-list-one", "topic-list-two"}, new[] {"topic-list-one", "topic-list-two"})]
        public void GetTopics_Tests(string topic, string[] topics, string[] expectedTopics)
        {
            var actualTopics = GetProvider(new WorkerConfig
            {
                Connectors = new Dictionary<string, ConnectorConfig>
                {
                    {"connector", new ConnectorConfig {Topic = topic, Topics = topics}}
                }
            }).GetTopics("connector");

            Assert.Equal(expectedTopics.Length, actualTopics.Count);
            Assert.All(expectedTopics, s => actualTopics.Contains(s));
        }

        

        [Theory]
        [MemberData(nameof(MessageProcessorsTests))]
        public void GetMessageProcessors_Tests(IDictionary<string, ProcessorConfig> processors,
            IList<ProcessorConfig> expectedProcessors)
        {
            var actual = GetProvider(new WorkerConfig
            {
                Connectors = new Dictionary<string, ConnectorConfig>
                {
                    {"connector", new ConnectorConfig {Processors = processors}}
                }
            }).GetMessageProcessors("connector", "test-topic");

            Assert.Equal(expectedProcessors.Count, actual.Count);
            Assert.All(expectedProcessors, config => actual.Any(a => a.Name == config.Name && a.Order == config.Order));
        }
        
        [Theory]
        [MemberData(nameof(SinkConfigTests))]
        public void GetSinkConfig_Tests(SinkConfig input, string connectorPlugin, SinkConfig expected)
        {
            var actual = GetProvider(new WorkerConfig
            {
                Connectors = new Dictionary<string, ConnectorConfig>
                {
                    {"connector", new ConnectorConfig {Sink = input, Plugin = connectorPlugin}}
                }
            }).GetSinkConfig("connector");

            Assert.NotNull(actual);
            Assert.Equal(expected.Handler, expected.Handler);
            Assert.Equal(expected.Plugin, expected.Plugin);
        }

        [Theory]
        [InlineData(ErrorTolerance.All, true)]
        [InlineData(ErrorTolerance.None, false)]
        public void IsErrorTolerated_Tests(ErrorTolerance input, bool expected)
        {
            var actual = GetProvider(new WorkerConfig
            {
                Connectors = new Dictionary<string, ConnectorConfig>
                {
                    {
                        "connector",
                        new ConnectorConfig {Retries = new RetryConfig {Errors = new ErrorsConfig {Tolerance = input}}}
                    }
                }
            }).IsErrorTolerated("connector");
            
            Assert.Equal(actual, expected);
        }
        
        [Theory]
        [InlineData(ErrorTolerance.None, null, false)]
        [InlineData(ErrorTolerance.All, null, false)]
        [InlineData(ErrorTolerance.All, "", false)]
        [InlineData(ErrorTolerance.All, "dead-letter-topic", true)]
        public void IsDeadLetterEnabled_Tests(ErrorTolerance tolerance, string deadLetterTopic,  bool expected)
        {
            var actual = GetProvider(new WorkerConfig
            {
                Connectors = new Dictionary<string, ConnectorConfig>
                {
                    {
                        "connector",
                        new ConnectorConfig
                        {
                            Retries = new RetryConfig
                                {Errors = new ErrorsConfig {Tolerance = tolerance, Topic = deadLetterTopic}}
                        }
                    }
                }
            }).IsDeadLetterEnabled("connector");
            
            Assert.Equal(actual, expected);
        }

        [Theory]
        [InlineData(null, null, false, false)]
        [InlineData(false, false, false, false)]
        [InlineData(true, true, true, true)]
        public void GetAutoCommitConfig_Tests(bool? inputEnableAutoCommit, bool? inputEnableAutoOffsetStore, bool expectedEnableAutoCommit, bool expectedEnableAutoOffsetStore)
        {
            var (enableAutoCommit, enableAutoOffsetStore) = GetProvider(new WorkerConfig
            {
                EnableAutoCommit = inputEnableAutoCommit,
                EnableAutoOffsetStore = inputEnableAutoOffsetStore
            }).GetAutoCommitConfig();
            
            Assert.Equal(expectedEnableAutoCommit, enableAutoCommit);
            Assert.Equal(expectedEnableAutoOffsetStore, enableAutoOffsetStore);
        }

        [Theory]
        [MemberData(nameof(ValidateConfigTests))]
        public void Validate_Tests( WorkerConfig workerConfig, string expected, bool throwsException = true)
        {
            var exception = Record.Exception(() => GetProvider(workerConfig).Validate());
            if (!throwsException)
            {
                Assert.Null(exception);
            }
            else
            {
                Assert.NotNull(exception);
                Assert.IsType<ArgumentException>(exception);
                Assert.Equal(expected, exception.Message);
            }
        }
        

        private ConfigurationProvider GetProvider(WorkerConfig config)
        {
            var options = Substitute.For<IOptions<WorkerConfig>>();
            options.Value.Returns(config);
            return new ConfigurationProvider(options);
        }

        public static IEnumerable<object[]> FailOverConfigTests
        {
            get
            {
                yield return new object[] {null, new FailOverConfig()};
                yield return new object[]
                {
                    new FailOverConfig
                    {
                        Disabled = true,
                        InitialDelayMs = 100,
                        PeriodicDelayMs = 100,
                        FailureThreshold = 2,
                        RestartDelayMs = 100
                    },
                    new FailOverConfig
                    {
                        Disabled = true,
                        InitialDelayMs = 100,
                        PeriodicDelayMs = 100,
                        FailureThreshold = 2,
                        RestartDelayMs = 100
                    }
                };
            }
        }

        public static IEnumerable<object[]> HealthCheckConfigTests
        {
            get
            {
                yield return new object[] {null, new HealthCheckConfig()};
                yield return new object[]
                {
                    new HealthCheckConfig()
                    {
                        Disabled = true,
                        InitialDelayMs = 100,
                        PeriodicDelayMs = 100,
                    },
                    new HealthCheckConfig()
                    {
                        Disabled = true,
                        InitialDelayMs = 100,
                        PeriodicDelayMs = 100
                    }
                };
            }
        }

        public static IEnumerable<object[]> RestartsConfigTests
        {
            get
            {
                yield return new object[] {null, new RestartsConfig()};
                yield return new object[]
                {
                    new RestartsConfig()
                    {
                        EnabledFor = RestartsLevel.Connector,
                        RetryWaitTimeMs = 100,
                        PeriodicDelayMs = 100,
                    },
                    new RestartsConfig()
                    {
                        EnabledFor = RestartsLevel.Connector,
                        RetryWaitTimeMs = 100,
                        PeriodicDelayMs = 100
                    }
                };
            }
        }

        public static IEnumerable<object[]> RetriesConfigTests
        {
            get
            {
                yield return new object[] {null, null, new RetryConfig()};
                yield return new object[]
                {
                    null, new RetryConfig {Attempts = 5, DelayTimeoutMs = 1000},
                    new RetryConfig {Attempts = 5, DelayTimeoutMs = 1000}
                };
                yield return new object[]
                {
                    new RetryConfig {Attempts = 10, DelayTimeoutMs = 2000},
                    new RetryConfig {Attempts = 5, DelayTimeoutMs = 1000},
                    new RetryConfig {Attempts = 10, DelayTimeoutMs = 2000}
                };
            }
        }

        public static IEnumerable<object[]> ErrorsConfigTests
        {
            get
            {
                yield return new object[] {null, null, new ErrorsConfig {Tolerance = ErrorTolerance.All}};
                yield return new object[] {null, new RetryConfig(), new ErrorsConfig {Tolerance = ErrorTolerance.All}};
                yield return new object[] {new RetryConfig(), null, new ErrorsConfig {Tolerance = ErrorTolerance.All}};
                yield return new object[]
                    {new RetryConfig(), new RetryConfig(), new ErrorsConfig {Tolerance = ErrorTolerance.All}};
                yield return new object[]
                {
                    null,
                    new RetryConfig
                        {Errors = new ErrorsConfig {Tolerance = ErrorTolerance.None, Topic = "worker-topic"}},
                    new ErrorsConfig {Tolerance = ErrorTolerance.None, Topic = "worker-topic"}
                };
                yield return new object[]
                {
                    new RetryConfig(),
                    new RetryConfig
                        {Errors = new ErrorsConfig {Tolerance = ErrorTolerance.None, Topic = "worker-topic"}},
                    new ErrorsConfig {Tolerance = ErrorTolerance.None, Topic = "worker-topic"}
                };
                yield return new object[]
                {
                    new RetryConfig
                        {Errors = new ErrorsConfig {Tolerance = ErrorTolerance.None, Topic = "connector-topic"}},
                    new RetryConfig
                        {Errors = new ErrorsConfig {Tolerance = ErrorTolerance.None, Topic = "worker-topic"}},
                    new ErrorsConfig {Tolerance = ErrorTolerance.None, Topic = "connector-topic"}
                };
            }
        }

        public static IEnumerable<object[]> EofSignalConfigTests
        {
            get
            {
                yield return new object[] {null, null, new EofConfig()};
                yield return new object[] {null, new BatchConfig(), new EofConfig()};
                yield return new object[] {new BatchConfig(), null, new EofConfig()};
                yield return new object[] {new BatchConfig(), new BatchConfig(), new EofConfig()};
                yield return new object[]
                {
                    null, new BatchConfig {EofSignal = new EofConfig {Enabled = true, Topic = "worker-topic"}},
                    new EofConfig {Enabled = true, Topic = "worker-topic"}
                };
                yield return new object[]
                {
                    new BatchConfig(),
                    new BatchConfig {EofSignal = new EofConfig {Enabled = true, Topic = "worker-topic"}},
                    new EofConfig {Enabled = true, Topic = "worker-topic"}
                };
                yield return new object[]
                {
                    new BatchConfig {EofSignal = new EofConfig {Enabled = true, Topic = "connector-topic"}},
                    new BatchConfig {EofSignal = new EofConfig {Enabled = true, Topic = "worker-topic"}},
                    new EofConfig {Enabled = true, Topic = "connector-topic"}
                };
            }
        }

        public static IEnumerable<object[]> BatchConfigTests
        {
            get
            {
                yield return new object[] {false, null, null, new BatchConfig {Size = 1, Parallelism = 1}};
                yield return new object[] {true, null, null, new BatchConfig()};
                yield return new object[]
                {
                    true, null, new BatchConfig {Size = 5, Parallelism = 50},
                    new BatchConfig {Size = 5, Parallelism = 50}
                };
                yield return new object[]
                {
                    true, new BatchConfig {Size = 100, Parallelism = 25}, new BatchConfig {Size = 5, Parallelism = 50},
                    new BatchConfig {Size = 100, Parallelism = 25}
                };
            }
        }

        public static IEnumerable<object[]> MessageConvertersTests
        {
            get
            {
                yield return new object[] {null, null, (Constants.DefaultDeserializer, Constants.DefaultDeserializer)};
                yield return new object[]
                    {null, new BatchConfig(), (Constants.DefaultDeserializer, Constants.DefaultDeserializer)};
                yield return new object[]
                    {new BatchConfig(), null, (Constants.DefaultDeserializer, Constants.DefaultDeserializer)};
                yield return new object[]
                {
                    new BatchConfig(), new BatchConfig(), (Constants.DefaultDeserializer, Constants.DefaultDeserializer)
                };
                yield return new object[]
                {
                    new BatchConfig(), new BatchConfig {Deserializers = new ConverterConfig()},
                    (Constants.DefaultDeserializer, Constants.DefaultDeserializer)
                };
                yield return new object[]
                {
                    new BatchConfig {Deserializers = new ConverterConfig()},
                    new BatchConfig {Deserializers = new ConverterConfig()},
                    (Constants.DefaultDeserializer, Constants.DefaultDeserializer)
                };
                yield return new object[]
                {
                    new BatchConfig {Deserializers = new ConverterConfig()},
                    new BatchConfig
                    {
                        Deserializers = new ConverterConfig
                            {Key = "worker-key-deserializer", Value = "worker-value-deserializer"}
                    },
                    ("worker-key-deserializer", "worker-value-deserializer")
                };
                yield return new object[]
                {
                    new BatchConfig {Deserializers = new ConverterConfig()},
                    new BatchConfig
                    {
                        Deserializers = new ConverterConfig
                        {
                            Key = "worker-key-deserializer", Value = "worker-value-deserializer",
                            Overrides = new List<ConverterOverrideConfig>
                            {
                                new()
                                {
                                    Topic = "test-topic", Key = "worker-key-override-deserializer",
                                    Value = "worker-value-override-deserializer"
                                }
                            }
                        }
                    },
                    ("worker-key-override-deserializer", "worker-value-override-deserializer")
                };
                yield return new object[]
                {
                    new BatchConfig
                    {
                        Deserializers = new ConverterConfig
                            {Key = "connector-key-deserializer", Value = "connector-value-deserializer"}
                    },
                    new BatchConfig
                    {
                        Deserializers = new ConverterConfig
                        {
                            Key = "worker-key-deserializer", Value = "worker-value-deserializer",
                            Overrides = new List<ConverterOverrideConfig>
                            {
                                new()
                                {
                                    Topic = "test-topic", Key = "worker-key-override-serializer",
                                    Value = "worker-value-override-serializer"
                                }
                            }
                        }
                    },
                    ("connector-key-deserializer", "connector-value-deserializer")
                };
                yield return new object[]
                {
                    new BatchConfig
                    {
                        Deserializers = new ConverterConfig
                        {
                            Key = "connector-key-deserializer", Value = "connector-value-deserializer",
                            Overrides = new List<ConverterOverrideConfig>
                            {
                                new()
                                {
                                    Topic = "test-topic", Key = "connector-key-override-deserializer",
                                    Value = "connector-value-override-deserializer"
                                }
                            }
                        }
                    },
                    new BatchConfig
                    {
                        Deserializers = new ConverterConfig
                        {
                            Key = "worker-key-deserializer", Value = "worker-value-deserializer",
                            Overrides = new List<ConverterOverrideConfig>
                            {
                                new()
                                {
                                    Topic = "test-topic", Key = "worker-key-override-deserializer",
                                    Value = "worker-value-override-deserializer"
                                }
                            }
                        }
                    },
                    ("connector-key-override-deserializer", "connector-value-override-deserializer")
                };
            }
        }

        public static IEnumerable<object[]> MessageProcessorsTests
        {
            get
            {
                yield return new object[] {null, new List<ProcessorConfig>()};
                yield return new object[] {new Dictionary<string, ProcessorConfig>(), new List<ProcessorConfig>()};
                yield return new object[]
                {
                    new Dictionary<string, ProcessorConfig>()
                    {
                        {"first-processor", new ProcessorConfig {Order = 1}},
                        {"another-processor", new ProcessorConfig {Name = "second-processor", Order = 1}}
                    },
                    new List<ProcessorConfig>
                        {new() {Name = "first-processor", Order = 1}, new() {Name = "second-processor", Order = 1}}
                };
                yield return new object[]
                {
                    new Dictionary<string, ProcessorConfig>()
                    {
                        {"first-processor", new ProcessorConfig {Order = 1, Topics = new[] {"some-other-topic"}}},
                        {"another-processor", new ProcessorConfig {Name = "second-processor", Order = 1}},
                        {"topic-first-processor", new ProcessorConfig {Order = 1, Topics = new[] {"test-topic"}}},
                        {
                            "another-topic-processor",
                            new ProcessorConfig
                            {
                                Name = "topic-second-processor", Order = 1,
                                Topics = new[] {"test-topic", "additional-topic"}
                            }
                        }
                    },
                    new List<ProcessorConfig>
                    {
                        new() {Name = "topic-first-processor", Order = 1}, new() {Name = "second-processor", Order = 1},
                        new() {Name = "topic-second-processor", Order = 1}
                    }
                };
            }
        }

        public static IEnumerable<object[]> SinkConfigTests
        {
            get
            {
                yield return new object[] {null, "connector-plugin", new SinkConfig {Plugin = "connector-plugin"}};
                yield return new object[]
                    {new SinkConfig(), "connector-plugin", new SinkConfig {Plugin = "connector-plugin"}};
                yield return new object[]
                {
                    new SinkConfig {Handler = "sink-handler", Plugin = "sink-plugin"}, "connector-plugin",
                    new SinkConfig {Handler = "sink-handler", Plugin = "sink-plugin"}
                };
            }
        }

        public static IEnumerable<object[]> ValidateConfigTests
        {
            get
            {
                yield return new object[] { new WorkerConfig{ BootstrapServers = null}, "Bootstrap Servers isn't configured for worker."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = ""}, "Bootstrap Servers isn't configured for worker."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = null} , "At least one connector is required for the worker to start."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>()} , "At least one connector is required for the worker to start."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>{ {"", null } }} , "Connector Name configuration property must be specified and must be unique."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>{ {"", new ConnectorConfig() } }} , "Connector Name configuration property must be specified and must be unique."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>{ {"first", new ConnectorConfig{Name = "same-name"}}, {"second", new ConnectorConfig{Name = "same-name"} } }} , "Connector Name configuration property must be specified and must be unique."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>{ {"first", new ConnectorConfig{Name = "same-name"}}, {"second", new ConnectorConfig{Name = "new-name"} } }, Plugins = null} , "At least one plugin is required for the worker to start."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>{ {"first", new ConnectorConfig{Name = "same-name"}}, {"second", new ConnectorConfig{Name = "new-name"} } }, Plugins = new PluginConfig()} , "At least one plugin is required for the worker to start."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>{ {"first", new ConnectorConfig{Name = "same-name"}}, {"second", new ConnectorConfig{Name = "new-name"} } }, Plugins = new PluginConfig{ Initializers = null}} , "At least one plugin is required for the worker to start."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>{ {"first", new ConnectorConfig{Name = "same-name"}}, {"second", new ConnectorConfig{Name = "new-name"} } }, Plugins = new PluginConfig{ Initializers = new Dictionary<string, InitializerConfig>()}} , "At least one plugin is required for the worker to start."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>{ {"first", new ConnectorConfig{Name = "same-name"}}, {"second", new ConnectorConfig{Name = "new-name"} } }, Plugins = new PluginConfig{ Initializers = new Dictionary<string, InitializerConfig>{{"", null }}}} , "Plugin Name configuration property must be specified and must be unique."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>{ {"connector-name", new ConnectorConfig{ Plugin = null} } }, Plugins = new PluginConfig{ Initializers = new Dictionary<string, InitializerConfig>{{"plugin-one", new InitializerConfig() }, {"plugin-two", new InitializerConfig() }}}} , "Connector: connector-name is not associated to any of the available Plugins: [ plugin-one, plugin-two ]."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>{ {"connector-name", new ConnectorConfig{ Plugin = "non-exiting-plugin"} } }, Plugins = new PluginConfig{ Initializers = new Dictionary<string, InitializerConfig>{{"plugin-one", new InitializerConfig() }, {"plugin-two", new InitializerConfig() }}}} , "Connector: connector-name is not associated to any of the available Plugins: [ plugin-one, plugin-two ]."};
                yield return new object[] { new WorkerConfig{ BootstrapServers = "bootstrap-server", Connectors = new Dictionary<string, ConnectorConfig>{ {"connector-name", new ConnectorConfig{ Plugin = "plugin-one"} } }, Plugins = new PluginConfig{ Initializers = new Dictionary<string, InitializerConfig>{{"plugin-one", new InitializerConfig() }, {"plugin-two", new InitializerConfig() }}}} , null, false};
            }
        }


    }
}