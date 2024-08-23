using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Connectors
{
    public class SourceTaskTests
    {
        private readonly IExecutionContext _executionContext;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IConnectRecordCollection _pollRecordCollection;
        private readonly ISinkExceptionHandler _sinkExceptionHandler;
        private readonly ITokenHandler _tokenHandler;
        private readonly ISourceTask _sourceTask;

        public SourceTaskTests()
        {
            _executionContext = Substitute.For<IExecutionContext>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _pollRecordCollection = Substitute.For<IConnectRecordCollection>();
            _sinkExceptionHandler = Substitute.For<ISinkExceptionHandler>();
            _tokenHandler = Substitute.For<ITokenHandler>();
            _sourceTask = new SourceTask(
                _executionContext,
                _configurationProvider,
                _pollRecordCollection,
                _tokenHandler);
        }
        
        [Fact]
        public async Task Execute_StopTaskIfSubscriberNotFound()
        {
            // Arrange
            const string connector = "test-connector";
            const int taskId = 1;
            var cts = GetCancellationToken();
            _pollRecordCollection.TrySubscribe().Returns(false);

            // Act
            await _sourceTask.Execute(connector, 1, cts);

            // Assert
            Received.InOrder(() =>
            {
                _executionContext.Initialize(connector, taskId, _sourceTask);
                _pollRecordCollection.Setup(ConnectorType.Source, connector, taskId);
                _pollRecordCollection.TrySubscribe();
            });
        }
        
        [Fact]
        public async Task Execute_StopTaskIfPublisherNotFound()
        {
            // Arrange
            const string connector = "test-connector";
            const int taskId = 1;
            var cts = GetCancellationToken();
            _pollRecordCollection.TrySubscribe().Returns(true);
            _pollRecordCollection.TryPublisher().Returns(false);

            // Act
            await _sourceTask.Execute(connector, 1, cts);

            // Assert
            Received.InOrder(() =>
            {
                _executionContext.Initialize(connector, taskId, _sourceTask);
                _pollRecordCollection.Setup(ConnectorType.Source, connector, taskId);
                _pollRecordCollection.TrySubscribe();
                _pollRecordCollection.TryPublisher();
            });
        }
        
        [Fact]
        public async Task Execute_StopTaskIfCancelledImmediately()
        {
            // Arrange
            const string connector = "test-connector";
            const int taskId = 1;
            var cts = GetCancellationToken(0);
            _pollRecordCollection.TrySubscribe().Returns(true);
            _pollRecordCollection.TryPublisher().Returns(true);
            _configurationProvider.GetBatchConfig(Arg.Any<string>()).Returns(new BatchConfig());

            // Act
            await _sourceTask.Execute(connector, 1, cts);

            // Assert
            Received.InOrder(() =>
            {
                _executionContext.Initialize(connector, taskId, _sourceTask);
                _pollRecordCollection.Setup(ConnectorType.Source, connector, taskId);
                _pollRecordCollection.TrySubscribe();
                _pollRecordCollection.TryPublisher();
                _configurationProvider.GetBatchConfig(connector);
                _configurationProvider.GetParallelRetryOptions(connector);
                _configurationProvider.GetBatchConfig(connector);
            });
        }
        
        private CancellationTokenSource GetCancellationToken(int loop = -1)
        {
            var cts = new CancellationTokenSource();

            _tokenHandler.When(k => k.NoOp()).Do(_ =>
            {
                if (loop-- == 0) cts.Cancel();
            });
            return cts;
        }
    }
}