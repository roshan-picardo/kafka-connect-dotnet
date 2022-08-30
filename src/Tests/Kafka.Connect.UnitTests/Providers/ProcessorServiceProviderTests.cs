using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Providers
{
    public class ProcessorServiceProviderTests
    {
        private IEnumerable<IProcessor> _processors;
        private IEnumerable<IDeserializer> _deserializers;
        private readonly ILogger<ProcessorServiceProvider> _logger;

        private ProcessorServiceProvider _processorServiceProvider;

        public ProcessorServiceProviderTests()
        {
            _processors = Substitute.For<IEnumerable<IProcessor>>();
            _deserializers = Substitute.For<IEnumerable<IDeserializer>>();
            _logger = Substitute.For<ILogger<ProcessorServiceProvider>>();
        }

        [Fact]
        public void GetProcessors_ReturnsListOfProcessors()
        {
            _processors = new[] {Substitute.For<IProcessor>(), Substitute.For<IProcessor>()};

            _processorServiceProvider =
                new ProcessorServiceProvider(_logger, _processors, _deserializers);

            var expected = _processorServiceProvider.GetProcessors();

            Assert.Equal(2, expected.Count());
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void GetDeserializer_Tests(bool exists)
        {
            _deserializers = new[] {Substitute.For<IDeserializer>()};
            _deserializers.First().IsOfType(Arg.Any<string>()).Returns(exists);

            _processorServiceProvider = new ProcessorServiceProvider(_logger, _processors, _deserializers);

            var expected = _processorServiceProvider.GetDeserializer("Kafka.Connect.Serializers.AvroDeserializer");

            if (exists)
                Assert.NotNull(expected);
            else
                Assert.Null(expected);
        }
    }
}