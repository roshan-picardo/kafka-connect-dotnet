using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Config;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Connectors
{
    public class ConnectorTests
    {
        private readonly ILogger<Connector> _logger;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        private readonly ISinkHandlerProvider _sinkHandlerProvider;
        private readonly IConnectDeadLetter _connectDeadLetter;
        private readonly ISinkHandler _sinkHandler;
        private readonly IServiceScope _serviceScope;
        private readonly IServiceProvider _serviceProvider;
        private readonly ISinkTask _sinkTask;
        
        private readonly Connector _connector;

        public ConnectorTests()
        {
            _logger = Substitute.For<ILogger<Connector>>();
            _serviceScopeFactory = Substitute.For<IServiceScopeFactory>();
            _sinkHandlerProvider = Substitute.For<ISinkHandlerProvider>();
            _connectDeadLetter = Substitute.For<IConnectDeadLetter>();
            _sinkHandler = Substitute.For<ISinkHandler>();
            _serviceScope = Substitute.For<IServiceScope>();
            _serviceProvider = Substitute.For<IServiceProvider>();
            _sinkTask = Substitute.For<ISinkTask>();
            _connector = new Connector(_logger, _serviceScopeFactory, _sinkHandlerProvider, null)
            {
            };
        }

        [Theory]
        [InlineData(null, 1)]
        [InlineData(-1, 1)]
        [InlineData(0, 1)]
        [InlineData(1, 1)]
        [InlineData(4, 4)]
        public async Task Start_Tests(int? maxTasks, int expectedTasks)
        {
            var connectorConfig = new ConnectorConfig
            {
                Name = "test.connector",
                Plugin = "test.plugin",
                MaxTasks = maxTasks,
                Sink = new SinkConfig
                {
                    Handler = "test.handler"
                },
                SelfHealing = new SelfHealingConfig()
            };
            
            var cts = new CancellationTokenSource();
            var pts = new PauseTokenSource();
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>(), Arg.Any<string>()).Returns(_sinkHandler);

            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);
            

            await _connector.Start(connectorConfig, cts, pts);
            
            // asserts calls made
           await _connectDeadLetter.Received(1).CreateTopic(connectorConfig);
            _sinkHandlerProvider.Received(1).GetSinkHandler(connectorConfig.Plugin, connectorConfig.Sink.Handler); 
            await _sinkHandler.Received(1).Startup(connectorConfig.Name);
           await _sinkHandler.Received(1).Cleanup(connectorConfig.Name);
           _logger.Received(1).LogInformation($"Starting tasks: {expectedTasks}");
           _serviceScopeFactory.Received(expectedTasks).CreateScope();
           _serviceProvider.Received(expectedTasks).GetService<ISinkTask>();
           for (var i = 0; i < expectedTasks; i++)
           {
               _logger.Received().LogInformation($"Starting the Task #{i:00}");
           }
        }
        
        
        [Theory]
        [InlineData(null, 1)]
        [InlineData(-1, 1)]
        [InlineData(0, 1)]
        [InlineData(1, 1)]
        [InlineData(4, 4)]        
        public async void Start_And_Cancel_Tests(int? maxTasks, int expectedTasks)
        {
            var connectorConfig = new ConnectorConfig()
            {
                Name = "test.connector",
                Plugin = "test.plugin",
                MaxTasks = maxTasks,
                Sink = new SinkConfig
                {
                    Handler = "test.handler"
                },
                SelfHealing = new SelfHealingConfig()
            };
            var cts = new CancellationTokenSource();
            var pts = new PauseTokenSource();

            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>(), Arg.Any<string>()).Returns(_sinkHandler);

            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);
            

            await _connector.Start(connectorConfig, cts, pts);
            
            cts.Cancel();
            
            // asserts calls made
            await _connectDeadLetter.Received(1).CreateTopic(connectorConfig);
            _sinkHandlerProvider.Received(1).GetSinkHandler(connectorConfig.Plugin, connectorConfig.Sink.Handler);
            await _sinkHandler.Received(1).Startup(connectorConfig.Name);
            await _sinkHandler.Received(1).Cleanup(connectorConfig.Name);
            _logger.Received(1).LogInformation($"Starting tasks: {expectedTasks}");
            _serviceScopeFactory.Received(expectedTasks).CreateScope();
            _serviceProvider.Received(expectedTasks).GetService<ISinkTask>();
            
            for (var i = 0; i < expectedTasks; i++)
            {
                _logger.Received().LogInformation($"Starting the Task #{i:00}");
                _logger.Received().LogInformation(
                    $"Shutting down the Task #{i:00} (Status: RanToCompletion)");
            }
        }
    }
}