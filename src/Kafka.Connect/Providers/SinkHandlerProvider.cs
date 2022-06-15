using System.Collections.Generic;
using System.Linq;
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

        public ISinkHandler GetSinkHandler(string connector)
        {
            var config = _configurationProvider.GetSinkConfig(connector);
            var sinkHandler = _sinkHandlers.SingleOrDefault(s => s.IsOfType(config.Plugin, config.Handler));
            _logger.LogTrace("{@Log}", new {Message = "Selected sink handler.", config.Plugin, Handler = sinkHandler?.GetType().FullName});
            return sinkHandler;
        }
    }
}