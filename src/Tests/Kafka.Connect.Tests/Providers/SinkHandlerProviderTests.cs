using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Providers
{
    public class SinkHandlerProviderTests 
    {
        private IEnumerable<ISinkHandler> _sinkHandlers;
        private ISinkHandlerProvider _sinkHandlerProvider;
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void GetValueDeserializer(bool exists)
        {
            _sinkHandlers = new[] {Substitute.For<ISinkHandler>()};
            _sinkHandlers.First().IsOfType(Arg.Any<string>(), Arg.Any<string>()).Returns(exists);

            _sinkHandlerProvider = new SinkHandlerProvider(Substitute.For<ILogger<SinkHandlerProvider>>(), _sinkHandlers);

            var expected = _sinkHandlerProvider.GetSinkHandler("plugin", "handler");

            if (exists)
                Assert.NotNull(expected);
            else
                Assert.Null(expected);
        }
    }
}