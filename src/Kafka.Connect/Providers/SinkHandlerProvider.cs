using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Providers
{
    public class SinkHandlerProvider : ISinkHandlerProvider
    {
        private readonly ILogger<SinkHandlerProvider> _logger;
        private readonly IEnumerable<ISinkHandler> _sinkHandlers;
        private readonly IConfigurationProvider _configurationProvider;

        public SinkHandlerProvider(ILogger<SinkHandlerProvider> logger, IEnumerable<ISinkHandler> sinkHandlers, IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _sinkHandlers = sinkHandlers;
            _configurationProvider = configurationProvider;
        }
        public ISinkHandler GetSinkHandler(string plugin, string handler)
        {
            var sinkHandler = _sinkHandlers.SingleOrDefault(s => s.IsOfType(plugin, handler));
            _logger.LogTrace("{@Log}", new {Message = "Selected sink handler.", Plugin = plugin, Handler = sinkHandler?.GetType().FullName});
            return sinkHandler;
        }
        
        public ISinkHandler GetSinkHandler(string connector)
        {
            var config = _configurationProvider.GetSinkConfig(connector);
            var sinkHandler = _sinkHandlers.SingleOrDefault(s => s.IsOfType(config.Handler, config.Handler));
            _logger.LogTrace("{@Log}", new {Message = "Selected sink handler.", Plugin = config.Plugin, Handler = sinkHandler?.GetType().FullName});
            return sinkHandler;
        }
    }
}