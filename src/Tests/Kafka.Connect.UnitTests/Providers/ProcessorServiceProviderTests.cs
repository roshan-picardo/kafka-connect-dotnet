using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Providers;
using Kafka.Connect.Serializers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Providers
{
    public class ProcessorServiceProviderTests
    {
        private IEnumerable<IProcessor> _processors;
        private IEnumerable<IDeserializer> _deserializers;
        private IEnumerable<ISerializer> _serializers;
        private readonly ILogger<ProcessorServiceProvider> _logger;

        private ProcessorServiceProvider _processorServiceProvider;

        public ProcessorServiceProviderTests()
        {
            _processors = Substitute.For<IEnumerable<IProcessor>>();
            _deserializers = Substitute.For<IEnumerable<IDeserializer>>();
            _serializers = Substitute.For<IEnumerable<ISerializer>>();
            _logger = Substitute.For<ILogger<ProcessorServiceProvider>>();
        }

        [Fact]
        public void GetProcessors_ReturnsListOfProcessors()
        {
            _processors = new[] {Substitute.For<IProcessor>(), Substitute.For<IProcessor>()};

            _processorServiceProvider =
                new ProcessorServiceProvider(_logger, _processors, _deserializers, _serializers);

            var expected = _processorServiceProvider.GetProcessors();

            Assert.Equal(2, expected.Count());
        }

        [Theory(Skip = "TODO")]
        [InlineData(true)]
        [InlineData(false)]
        public void GetDeserializer_Tests(bool exists)
        {
            _deserializers = new[] {Substitute.For<IDeserializer>()};
            _deserializers.First().GetType().Returns(typeof(AvroDeserializer));

            _processorServiceProvider = new ProcessorServiceProvider(_logger, _processors, _deserializers, _serializers);

            var expected = _processorServiceProvider.GetDeserializer("Kafka.Connect.Serializers.AvroDeserializer");

            if (exists)
                Assert.NotNull(expected);
            else
                Assert.Null(expected);
        }
    }
}