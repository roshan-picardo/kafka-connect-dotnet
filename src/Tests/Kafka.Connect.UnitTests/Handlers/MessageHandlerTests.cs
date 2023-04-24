using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Providers;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Handlers
{
    public class MessageHandlerTests
    {
        private readonly ILogger<MessageHandler> _logger;
        private readonly IRecordFlattener _recordFlattener;
        private readonly IProcessorServiceProvider _processorServiceProvider;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IMessageHandler _messageHandler;
        private IProcessor _processor;

        public MessageHandlerTests()
        {
            _logger = Substitute.For<ILogger<MessageHandler>>();
            _recordFlattener = Substitute.For<IRecordFlattener>();
            _processorServiceProvider = Substitute.For<IProcessorServiceProvider>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _messageHandler = new MessageHandler(_logger, _recordFlattener, _processorServiceProvider, _configurationProvider);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Process_WhenConfigurationIsEmptyOrNull(bool isNull)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                }
            })
            {
                Message = new JObject {{"field", "test.value"}},
                Skip = true
            };

            _configurationProvider.GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>())
                .Returns(isNull ? null : new List<ProcessorConfig>());

            var (skip, data) = await _messageHandler.Process(sinkRecord, "");
            
            Assert.Equal(sinkRecord.Skip, skip);
            Assert.Equal(sinkRecord.Message, data);
            _recordFlattener.DidNotReceive().Flatten(Arg.Any<JToken>());
            _recordFlattener.DidNotReceive().Unflatten(Arg.Any<IDictionary<string, object>>());
            _processorServiceProvider.DidNotReceive().GetProcessors();
            _configurationProvider.Received().GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>());
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Process_WhenProcessorsListIsEmptyOrNull(bool isNull)
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                }
            })
            {
                Message = new JObject {{"field", "test.value"}},
                Skip = true
            };

            _configurationProvider.GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>())
                .Returns(new List<ProcessorConfig>() {new() {Name = "firstProcessor"}});
            _processorServiceProvider.GetProcessors().Returns(isNull ? null : new List<IProcessor>());

            var (skip, data) = await _messageHandler.Process(sinkRecord, "");
            
            Assert.Equal(sinkRecord.Skip, skip);
            Assert.Equal(sinkRecord.Message, data);
            _recordFlattener.DidNotReceive().Flatten(Arg.Any<JToken>());
            _recordFlattener.DidNotReceive().Unflatten(Arg.Any<IDictionary<string, object>>());
            _processorServiceProvider.Received().GetProcessors();
            _configurationProvider.Received().GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>());
        }
        
        [Fact]
        public async Task Process_WhenConfiguredProcessorIsNotRegistered()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                }
            })
            {
                Message = new JObject {{"field", "test.value"}},
                Skip = true
            };
            _processor = Substitute.For<IProcessor>();
            _processor.IsOfType(Arg.Any<string>()).Returns(false);
            _configurationProvider.GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>())
                .Returns(new List<ProcessorConfig>() {new() {Name = "Kafka.Connect.Processors.Unknown"}});
            _processorServiceProvider.GetProcessors().Returns( new List<IProcessor> {_processor});
            _recordFlattener.Flatten(Arg.Any<JToken>()).Returns(ImmutableDictionary<string, object>.Empty);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(sinkRecord.Message);
            
            var (skip, data) = await _messageHandler.Process(sinkRecord, "");
            
            Assert.False(skip);
            Assert.Equal(sinkRecord.Message, data);
            _processor.Received().IsOfType(Arg.Any<string>());
            _recordFlattener.Received().Flatten(Arg.Any<JToken>());
            _recordFlattener.Received().Unflatten(Arg.Any<IDictionary<string, object>>());
            _processorServiceProvider.Received().GetProcessors();
            _configurationProvider.Received().GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>());
            _logger.Received().Trace("Processor is not registered.", Arg.Any<object>());
        }
        
        [Fact]
        public async Task Process_ApplyAllProcessors()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                }
            })
            {
                Message = new JObject {{"field", "test.value"}},
                Skip = true
            };
            _processor = Substitute.For<IProcessor>();
            _processor.IsOfType(Arg.Any<string>()).Returns(true);
            _configurationProvider.GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>())
                .Returns(new List<ProcessorConfig> {new() {Name = "Kafka.Connect.Processors.One"}, new() {Name = "Kafka.Connect.Processors.Two"}});
            _processorServiceProvider.GetProcessors().Returns( new List<IProcessor> {_processor});
            _recordFlattener.Flatten(Arg.Any<JToken>()).Returns(ImmutableDictionary<string, object>.Empty);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(sinkRecord.Message);
            
            var (skip, data) = await _messageHandler.Process(sinkRecord, "");
            
            Assert.False(skip);
            Assert.Equal(sinkRecord.Message, data);
            _processor.Received(2).IsOfType(Arg.Any<string>());
            await _processor.Received(2).Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<string>());
            _recordFlattener.Received().Flatten(Arg.Any<JToken>());
            _recordFlattener.Received().Unflatten(Arg.Any<IDictionary<string, object>>());
            _processorServiceProvider.Received().GetProcessors();
            _configurationProvider.Received().GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>());
        }
        
        [Fact]
        public async Task Process_SkipsAfterFirst()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                }
            })
            {
                Message = new JObject {{"field", "test.value"}},
                Skip = true
            };
            var pExecute = Substitute.For<IProcessor>();
            var pSkip = Substitute.For<IProcessor>();
            pExecute.IsOfType("Kafka.Connect.Processors.Execute").Returns(true);
            pSkip.IsOfType("Kafka.Connect.Processors.Skip").Returns(true);
            _configurationProvider.GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>())
                .Returns(new List<ProcessorConfig> {new() {Name = "Kafka.Connect.Processors.Execute"}, new() {Name = "Kafka.Connect.Processors.Skip"}});
            _processorServiceProvider.GetProcessors().Returns( new List<IProcessor> {pExecute, pSkip});
            pExecute.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<string>()).Returns((true, new Dictionary<string, object>()));
            _recordFlattener.Flatten(Arg.Any<JToken>()).Returns(ImmutableDictionary<string, object>.Empty);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(sinkRecord.Message);
            
            var (skip, data) = await _messageHandler.Process(sinkRecord, "");
            
            Assert.True(skip);
            Assert.Equal(sinkRecord.Message, data);
            await pExecute.Received().Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<string>());
            await pSkip.DidNotReceive().Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<string>());
            _recordFlattener.Received().Flatten(Arg.Any<JToken>());
            _recordFlattener.Received().Unflatten(Arg.Any<IDictionary<string, object>>());
            _processorServiceProvider.Received().GetProcessors();
            _configurationProvider.Received().GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>());
            _logger.Received().Trace("Message will be skipped from further processing.");
        }
        
        
        [Fact]
        public async Task Process_LoopAll_ExecuteNotFoundAndSkip()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                }
            })
            {
                Message = new JObject {{"field", "test.value"}},
                Skip = true
            };
            var pNotFound = Substitute.For<IProcessor>();
            var pExecute = Substitute.For<IProcessor>();
            var pSkip = Substitute.For<IProcessor>();
            pNotFound.IsOfType("Kafka.Connect.Processors.NotFound").Returns(false);
            pExecute.IsOfType("Kafka.Connect.Processors.Execute").Returns(true);
            pSkip.IsOfType("Kafka.Connect.Processors.Skip").Returns(true);
            _configurationProvider.GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>())
                .Returns(new List<ProcessorConfig> {new() {Name = "Kafka.Connect.Processors.NotFound"}, new() {Name = "Kafka.Connect.Processors.Execute"}, new() {Name="Kafka.Connect.Processors.Skip"}});
            _processorServiceProvider.GetProcessors().Returns( new List<IProcessor> {pNotFound, pExecute, pSkip});
            pExecute.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<string>()).Returns((true, new Dictionary<string, object>()));
            _recordFlattener.Flatten(Arg.Any<JToken>()).Returns(ImmutableDictionary<string, object>.Empty);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(sinkRecord.Message);
            
            var (skip, data) = await _messageHandler.Process(sinkRecord, "");
            
            Assert.True(skip);
            Assert.Equal(sinkRecord.Message, data);
            await pExecute.Received().Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<string>());
            await pNotFound.DidNotReceive().Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<string>());
            await pSkip.DidNotReceive().Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<string>());
            _recordFlattener.Received().Flatten(Arg.Any<JToken>());
            _recordFlattener.Received().Unflatten(Arg.Any<IDictionary<string, object>>());
            _processorServiceProvider.Received().GetProcessors();
            _configurationProvider.Received().GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>());
            _logger.Received().Trace("Message will be skipped from further processing.");
            _logger.Received().Trace("Processor is not registered.", Arg.Any<object>());
        }
        
        
        [Fact]
        public async Task Process_LoopAll_MaintainsOrder()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>()
                {
                    Headers = new Headers()
                }
            })
            {
                Message = new JObject {{"field", "test.value"}},
                Skip = true
            };
            var pSecond = Substitute.For<IProcessor>();
            var pFirst = Substitute.For<IProcessor>();
            var pThird = Substitute.For<IProcessor>();
            pSecond.IsOfType("Kafka.Connect.Processors.Second").Returns(true);
            pThird.IsOfType("Kafka.Connect.Processors.Third").Returns(true);
            pFirst.IsOfType("Kafka.Connect.Processors.First").Returns(true);
            _configurationProvider.GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>())
                .Returns(new List<ProcessorConfig> {new() {Name = "Kafka.Connect.Processors.Second", Order = 2}, new() {Name = "Kafka.Connect.Processors.Third", Order = 3}, new() {Name="Kafka.Connect.Processors.First", Order = 1}});
            _processorServiceProvider.GetProcessors().Returns( new List<IProcessor> {pSecond, pThird, pFirst});
            _recordFlattener.Flatten(Arg.Any<JToken>()).Returns(ImmutableDictionary<string, object>.Empty);
            _recordFlattener.Unflatten(Arg.Any<IDictionary<string, object>>()).Returns(sinkRecord.Message);
            
            var (skip, data) = await _messageHandler.Process(sinkRecord, "");
            
            Assert.False(skip);
            Assert.Equal(sinkRecord.Message, data);
            Received.InOrder(() =>
            {
                pFirst.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<string>());
                pSecond.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<string>());
                pThird.Apply(Arg.Any<IDictionary<string, object>>(), Arg.Any<string>());
            });
            _recordFlattener.Received().Flatten(Arg.Any<JToken>());
            _recordFlattener.Received().Unflatten(Arg.Any<IDictionary<string, object>>());
            _processorServiceProvider.Received().GetProcessors();
            _configurationProvider.Received().GetMessageProcessors(Arg.Any<string>(), Arg.Any<string>());
        }
    }
}