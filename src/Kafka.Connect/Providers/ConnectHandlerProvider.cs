using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Logging;

namespace Kafka.Connect.Providers
{
    public class ConnectHandlerProvider : IConnectHandlerProvider
    {
        private readonly ILogger<ConnectHandlerProvider> _logger;
        private readonly IEnumerable<ISinkHandler> _sinkHandlers;
        private readonly IEnumerable<ISourceHandler> _sourceHandlers;
        private readonly IConfigurationProvider _configurationProvider;

        public ConnectHandlerProvider(
            ILogger<ConnectHandlerProvider> logger,
            IEnumerable<ISinkHandler> sinkHandlers,
            IEnumerable<ISourceHandler> sourceHandlers,
            IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _sinkHandlers = sinkHandlers;
            _sourceHandlers = sourceHandlers;
            _configurationProvider = configurationProvider;
        }

        public ISinkHandler GetSinkHandler(string connector)
        {
            var config = _configurationProvider.GetSinkConfig(connector);
            var sinkHandler = _sinkHandlers.SingleOrDefault(s => s.Is(connector, config.Plugin, config.Handler));
            _logger.Trace("Selected sink handler.", new { config.Plugin, Handler = sinkHandler?.GetType().FullName });
            return sinkHandler;
        }

        public ISourceHandler GetSourceHandler(string connector)
        {
            var config = _configurationProvider.GetSourceConfig(connector);
            var sourceHandler = _sourceHandlers.SingleOrDefault(s => s.Is(connector, config.Plugin, config.Handler));
            _logger.Trace("Selected source handler.", new { config.Plugin, Handler = sourceHandler?.GetType().FullName });
            return sourceHandler;
        }
    }
}