using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Config;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Handlers
{
    public class MessageHandlerTests
    {
        private readonly IRecordFlattener _recordFlattener;
        private readonly IProcessorServiceProvider _processorServiceProvider;
        private readonly IMessageHandler _messageHandler;

        public MessageHandlerTests()
        {
            _recordFlattener = Substitute.For<IRecordFlattener>();
            _processorServiceProvider = Substitute.For<IProcessorServiceProvider>();
            _messageHandler = new MessageHandler(Substitute.For<ILogger<MessageHandler>>(), _recordFlattener,
                _processorServiceProvider);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Process_When_EmptyProcessorConfigs(bool isNull)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                }
            });
            var flattened = new Dictionary<string, object>
            {
                {"field", "test.value"}
            };
            var unflattened = new JObject {{"field", "test.value"}};
            _recordFlattener.Flatten(Arg.Any<JObject>()).Returns(flattened);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(unflattened);

            var (actualSkip, actualData) =
                await _messageHandler.Process(sinkRecord, new ConnectorConfig());
            
            Assert.Equal(sinkRecord.Skip, actualSkip);
            Assert.Equal(sinkRecord.Data, actualData);
            _recordFlattener.DidNotReceive().Flatten(Arg.Any<JToken>());
            _recordFlattener.DidNotReceive().Unflatten(Arg.Any<IDictionary<string, object>>());
            _processorServiceProvider.DidNotReceive().GetProcessors();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Process_When_ProcessorConfigsNutNoProcessorsRegistered(bool isNull)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                }
            });
            var flattened = new Dictionary<string, object>
            {
                {"field", "test.value"}
            };
            var unflattened = new JObject {{"field", "test.value"}};
            _recordFlattener.Flatten(Arg.Any<JObject>()).Returns(flattened);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(unflattened);
            _processorServiceProvider.GetProcessors().Returns(isNull ? null : new List<IProcessor>());

            var (actualSkip, actualData) =
                await _messageHandler.Process(sinkRecord, new ConnectorConfig());
            
            Assert.Equal(sinkRecord.Skip, actualSkip);
            Assert.Equal(sinkRecord.Data, actualData);
            _recordFlattener.DidNotReceive().Flatten(Arg.Any<JToken>());
            _recordFlattener.DidNotReceive().Unflatten(Arg.Any<IDictionary<string, object>>());
            _processorServiceProvider.Received().GetProcessors();
        }


        [Fact]
        public async Task Process_When_TopicSpecificProcessorNotFound()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                },
                Topic = "TopicA"
            });
            var flattened = new Dictionary<string, object>
            {
                {"field", "test.value"}
            };
            var unflattened = new JObject {{"field", "test.value"}};
            var processorConfigs = new[]
            {
                new ProcessorConfig()
                {
                    Topic = "TopicB"
                }
            };
            _recordFlattener.Flatten(Arg.Any<JObject>()).Returns(flattened);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(unflattened);
            _processorServiceProvider.GetProcessors().Returns(new[] {Substitute.For<IProcessor>()});
            
            var (actualSkip, actualData) = await _messageHandler.Process(sinkRecord, new ConnectorConfig());
            
            Assert.Equal(sinkRecord.Skip, actualSkip);
            Assert.Equal(unflattened, actualData);
            _recordFlattener.Received().Flatten(Arg.Any<JToken>());
            _recordFlattener.Received().Unflatten(Arg.Any<IDictionary<string, object>>());
            _processorServiceProvider.Received().GetProcessors();
        }


        [Fact]
        public async Task Process_When_TopicSpecificProcessorNotDefinedAndProcessorNotFound()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                },
                Topic = "TopicA"
            });
            var flattened = new Dictionary<string, object>
            {
                {"field", "test.value"}
            };
            var unflattened = new JObject {{"field", "test.value"}};
            var processor = Substitute.For<IProcessor>();
            var processorConfigs = new[]
            {
                new ProcessorConfig()
            };
            _recordFlattener.Flatten(Arg.Any<JObject>()).Returns(flattened);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(unflattened);
            _processorServiceProvider.GetProcessors().Returns(new[] {processor});
            processor.IsOfType(Arg.Any<string>()).Returns(false);
            
            var (actualSkip, actualData) = await _messageHandler.Process(sinkRecord, null);
            
            Assert.Equal(sinkRecord.Skip, actualSkip);
            Assert.Equal(unflattened, actualData);
            _recordFlattener.Received().Flatten(Arg.Any<JToken>());
            _recordFlattener.Received().Unflatten(Arg.Any<IDictionary<string, object>>());
            processor.Received().IsOfType(Arg.Any<string>());
            await processor.DidNotReceive().Apply(Arg.Any<IDictionary<string, object>>(),
                @"Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>()");
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task Process_When_ProcessorExistsOptionsMapsSetToNullOrEmpty(bool isOptionsNull,
            bool isMapsNull)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                },
                Topic = "TopicA"
            });
            var flattened = new Dictionary<string, object>
            {
                {"field", "test.value"}
            };
            var unflattened = new JObject {{"field", "test.value"}};
            var processor = Substitute.For<IProcessor>();
            var processorConfigs = new[]
            {
                new ProcessorConfig()
                {
                    //Options = isOptionsNull ? null : new List<string>(),
                    //Maps = isMapsNull ? null : new Dictionary<string, string>()
                }
            };
            _recordFlattener.Flatten(Arg.Any<JObject>()).Returns(flattened);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(unflattened);
            _processorServiceProvider.GetProcessors().Returns(new[] {processor});
            processor.IsOfType(Arg.Any<string>()).Returns(true);
            //processor.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<IEnumerable<string>>(),
            //    Arg.Any<IDictionary<string, string>>()).Returns((sinkRecord.Skip, flattened));

            var (actualSkip, actualData) = await _messageHandler.Process(sinkRecord, new ConnectorConfig());

            Assert.Equal(sinkRecord.Skip, actualSkip);
            Assert.Equal(unflattened, actualData);
            _recordFlattener.Received().Flatten(Arg.Any<JToken>());
            _recordFlattener.Received().Unflatten(Arg.Any<IDictionary<string, object>>());
            processor.Received().IsOfType(Arg.Any<string>());
            //await processor.Received().Apply(Arg.Any<IDictionary<string, object>>(),
             //   isOptionsNull ? null : Arg.Any<IEnumerable<string>>(),
             //   isMapsNull ? null : Arg.Any<IDictionary<string, string>>());
        }

        [Theory]
        [InlineData("key.fieldName", "key.fieldName")]
        [InlineData("value.fieldName", "value.fieldName")]
        [InlineData("fieldName", "value.fieldName")]
        public async Task Process_When_KeyPrefixApplied(string inputKey, string expectedKey)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                },
                Topic = "TopicA"
            });
            var flattened = new Dictionary<string, object>
            {
                {"field", "test.value"}
            };
            var unflattened = new JObject {{"field", "test.value"}};
            var processor = Substitute.For<IProcessor>();
            var processorConfigs = new[]
            {
                new ProcessorConfig
                {
                    //Options = new[] {inputKey},
                    //Maps = new Dictionary<string, string> {{inputKey, "SomeValue"}}
                }
            };
            _recordFlattener.Flatten(Arg.Any<JObject>()).Returns(flattened);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(unflattened);
            _processorServiceProvider.GetProcessors().Returns(new[] {processor});
            processor.IsOfType(Arg.Any<string>()).Returns(true);
                //processor.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<IEnumerable<string>>(),
               // Arg.Any<IDictionary<string, string>>()).Returns((sinkRecord.Skip, flattened));

            var (actualSkip, actualData) = await _messageHandler.Process(sinkRecord, null);

            Assert.Equal(sinkRecord.Skip, actualSkip);
            Assert.Equal(unflattened, actualData);
            _recordFlattener.Received().Flatten(Arg.Any<JToken>());
            _recordFlattener.Received().Unflatten(Arg.Any<IDictionary<string, object>>());
            processor.Received().IsOfType(Arg.Any<string>());
            //await processor.Received().Apply(Arg.Any<IDictionary<string, object>>(),
            //    Arg.Is<IEnumerable<string>>(s => s.Contains(expectedKey)),
             //   Arg.Is<IDictionary<string, string>>(d => d.ContainsKey(expectedKey)));
        }

        [Theory]
        [InlineData(false, false, false, false)]
        [InlineData(false, false, true, true)]
        [InlineData(false, true, false, true)]
        [InlineData(true, false, true, true)]
        public async Task Process_When_MultipleProcessorsWithSkip(bool p1Skip, bool p2Skip, bool p3Skip,
            bool expectedSkip)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                },
                Topic = "TopicA"
            });
            var flattened = new Dictionary<string, object>
            {
                {"field", "test.value"}
            };
            var unflattened = new JObject {{"field", "test.value"}};
            var processor1 = Substitute.For<IProcessor>();
            var processor2 = Substitute.For<IProcessor>();
            var processor3 = Substitute.For<IProcessor>();
            var processorConfigs = new[]
            {
                new ProcessorConfig {Name = "processor1"},
                new ProcessorConfig {Name = "processor2"},
                new ProcessorConfig {Name = "processor3"}
            };
            _recordFlattener.Flatten(Arg.Any<JObject>()).Returns(flattened);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(unflattened);
            _processorServiceProvider.GetProcessors().Returns(new[] {processor1, processor2, processor3});
            processor1.IsOfType("processor1").Returns(true);
            processor2.IsOfType("processor2").Returns(true);
            processor3.IsOfType("processor3").Returns(true);
            /*processor1.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>()).Returns((p1Skip, flattened));
            processor2.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>()).Returns((p2Skip, flattened));
            processor3.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>()).Returns((p3Skip, flattened));*/

            var (actualSkip, actualData) = await _messageHandler.Process(sinkRecord, null);

            Assert.Equal(expectedSkip, actualSkip);
            Assert.Equal(unflattened, actualData);
            _recordFlattener.Received().Flatten(Arg.Any<JToken>());
            _recordFlattener.Received().Unflatten(Arg.Any<IDictionary<string, object>>());
            processor1.Received().IsOfType(Arg.Any<string>());
            /*await processor1.Received(1).Apply(Arg.Any<IDictionary<string, object>>(),
                Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>());
            await processor2.Received(p1Skip ? 0 : 1).Apply(Arg.Any<IDictionary<string, object>>(),
                Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>());
            await processor3.Received(p1Skip || p2Skip ? 0 : 1).Apply(Arg.Any<IDictionary<string, object>>(),
                Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>());*/
        }


        [Fact]
        public async Task Process_When_MultipleProcessorsWithTopicConfig()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                },
                Topic = "TopicA"
            });
            var flattened = new Dictionary<string, object>
            {
                {"field", "test.value"}
            };
            var unflattened = new JObject {{"field", "test.value"}};
            var processor1 = Substitute.For<IProcessor>();
            var processor2 = Substitute.For<IProcessor>();
            var processor3 = Substitute.For<IProcessor>();
            var processorConfigs = new[]
            {
                new ProcessorConfig {Name = "processor1"},
                new ProcessorConfig {Name = "processor2", Topic = "TopicB"},
                new ProcessorConfig {Name = "processor3", Topic = "TopicA"}
            };
            _recordFlattener.Flatten(Arg.Any<JObject>()).Returns(flattened);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(unflattened);
            _processorServiceProvider.GetProcessors().Returns(new[] {processor1, processor2, processor3});
            processor1.IsOfType("processor1").Returns(true);
            processor2.IsOfType("processor2").Returns(true);
            processor3.IsOfType("processor3").Returns(true);
            /*processor1.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>()).Returns((sinkRecord.Skip, flattened));
            processor2.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>()).Returns((sinkRecord.Skip, flattened));
            processor3.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>()).Returns((sinkRecord.Skip, flattened));*/

            var (actualSkip, actualData) = await _messageHandler.Process(sinkRecord, null);

            Assert.Equal(sinkRecord.Skip, actualSkip);
            Assert.Equal(unflattened, actualData);
            _recordFlattener.Received().Flatten(Arg.Any<JToken>());
            _recordFlattener.Received().Unflatten(Arg.Any<IDictionary<string, object>>());
            processor1.Received().IsOfType(Arg.Any<string>());
            /*await processor1.Received().Apply(Arg.Any<IDictionary<string, object>>(),
                Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>());
            await processor2.DidNotReceive().Apply(Arg.Any<IDictionary<string, object>>(),
                Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>());
            await processor3.Received().Apply(Arg.Any<IDictionary<string, object>>(),
                Arg.Any<IEnumerable<string>>(),
                Arg.Any<IDictionary<string, string>>());*/
        }
    }
}