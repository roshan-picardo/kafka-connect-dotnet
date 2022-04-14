using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Config;
using Kafka.Connect.Connectors;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Handlers
{
    public class SinkExceptionHandlerTests
    {
        private readonly ILogger<SinkExceptionHandler> _logger;
        private readonly IConnectDeadLetter _connectDeadLetter;

        private readonly SinkExceptionHandler _sinkExceptionHandler;

        public SinkExceptionHandlerTests()
        {
            _logger = Substitute.For<ILogger<SinkExceptionHandler>>();
            _connectDeadLetter = Substitute.For<IConnectDeadLetter>();

            _sinkExceptionHandler = new SinkExceptionHandler(_logger, _connectDeadLetter);
        }

        [Fact]
        public async Task Handle_When_ConnectToleranceExceededExceptionRaised()
        {
            var sinkRecord = new SinkRecord(new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>() {Headers = new Headers()}, Topic = "TopicA",
            })
            {
                PublishMessages =
                    new Dictionary<string, Message<byte[], byte[]>>() {{"target-topic", new Message<byte[], byte[]>()}}
            };
            var batch = new SinkRecordBatch("unit-tests") {sinkRecord};

            var config = new ConnectorConfig
            {
                Errors = null
            };
            var je = new JsonException("Unit tests Json Exception");
            var exception = new ConnectToleranceExceededException(ErrorCode.Unknown, batch, je);
            var token = new CancellationTokenSource();

            _sinkExceptionHandler.Handle(config, exception, () => { token.Cancel(); });
        }
    }
}