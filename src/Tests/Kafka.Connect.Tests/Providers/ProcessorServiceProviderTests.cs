using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Processors;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Providers
{
    public class ProcessorServiceProviderTests
    {
        private IEnumerable<IProcessor> _processors;
        private IEnumerable<IDeserializer> _deserializers;
        private readonly IEnumerable<ISerializer> _serializers;
        private readonly ILogger<ProcessorServiceProvider> _logger;
        private IEnumerable<IEnricher> _enrichers;

        private ProcessorServiceProvider _processorServiceProvider;

        public ProcessorServiceProviderTests()
        {
            _processors = Substitute.For<IEnumerable<IProcessor>>();
            _deserializers = Substitute.For<IEnumerable<IDeserializer>>();
            _serializers = Substitute.For<IEnumerable<ISerializer>>();
            _logger = Substitute.For<ILogger<ProcessorServiceProvider>>();
        }

        [Fact]
        public void GetProcessors_Returns_ListOfProcessors()
        {
            _processors = new[] {Substitute.For<IProcessor>(), Substitute.For<IProcessor>()};

            _processorServiceProvider =
                new ProcessorServiceProvider(_logger, _processors, _deserializers, _serializers, _enrichers);

            var expected = _processorServiceProvider.GetProcessors();

            Assert.Equal(2, expected.Count());
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void GetKeyDeserializer(bool exists)
        {
            _deserializers = new[] {Substitute.For<IDeserializer>()};
            _deserializers.First().IsOfType(Arg.Any<string>()).Returns(exists);

            _processorServiceProvider = new ProcessorServiceProvider(_logger, _processors, _deserializers, _serializers, _enrichers);

            var expected = _processorServiceProvider.GetKeyDeserializer(new ConverterConfig());

            if (exists)
                Assert.NotNull(expected);
            else
                Assert.Null(expected);
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void GetValueDeserializer(bool exists)
        {
            _deserializers = new[] {Substitute.For<IDeserializer>()};
            _deserializers.First().IsOfType(Arg.Any<string>()).Returns(exists);

            _processorServiceProvider = new ProcessorServiceProvider(_logger, _processors, _deserializers, _serializers, _enrichers);

            var expected = _processorServiceProvider.GetValueDeserializer(new ConverterConfig());

            if (exists)
                Assert.NotNull(expected);
            else
                Assert.Null(expected);
        }
    }
}