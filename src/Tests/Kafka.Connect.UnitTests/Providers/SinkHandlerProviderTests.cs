using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Providers
{
    public class SinkHandlerProviderTests 
    {
        private IEnumerable<ISinkHandler> _sinkHandlers;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly ILogger<SinkHandlerProvider> _logger;
        private ISinkHandlerProvider _sinkHandlerProvider;

        public SinkHandlerProviderTests()
        {
            _logger = Substitute.For<ILogger<SinkHandlerProvider>>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void GetSinkHandler_Tests(bool exists)
        {
            _configurationProvider.GetSinkConfig(Arg.Any<string>()).Returns(new SinkConfig() {Plugin = "plugin"});

            _sinkHandlers = new[] {Substitute.For<ISinkHandler>()};

            _sinkHandlers.First().Is(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>()).Returns(exists);

            _sinkHandlerProvider = new SinkHandlerProvider(_logger, _sinkHandlers, _configurationProvider);

            var expected = _sinkHandlerProvider.GetSinkHandler("connector");

            if (exists)
            {
                Assert.NotNull(expected);
                _logger.Received().Trace("Selected sink handler.", Arg.Any<object>());
            }
            else
            {
                Assert.Null(expected);
                _logger.Received().Trace("Selected sink handler.", Arg.Any<object>());
            }
        }
    }
}