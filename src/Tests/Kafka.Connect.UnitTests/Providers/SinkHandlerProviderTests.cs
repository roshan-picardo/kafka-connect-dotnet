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
        private IEnumerable<ISourceHandler> _sourceHandlers;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly ILogger<ConnectHandlerProvider> _logger;
        private IConnectHandlerProvider _sinkHandlerProvider;

        public SinkHandlerProviderTests()
        {
            _logger = Substitute.For<ILogger<ConnectHandlerProvider>>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void GetSinkHandler_Tests(bool exists)
        {
            _configurationProvider.GetPluginConfig(Arg.Any<string>()).Returns(new PluginConfig() {Name = "plugin"});

            _sinkHandlers = new[] {Substitute.For<ISinkHandler>()};

            _sinkHandlers.First().Is(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>()).Returns(exists);

            _sinkHandlerProvider = new ConnectHandlerProvider(_logger, _sinkHandlers, _sourceHandlers, _configurationProvider);

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