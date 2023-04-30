using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace UnitTests.Kafka.Connect.Connectors
{
    public class ConnectorTests
    {
        private readonly ILogger<Connector> _logger;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        private readonly ISinkHandlerProvider _sinkHandlerProvider;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IExecutionContext _executionContext;
        private readonly ITokenHandler _tokenHandler;
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
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _executionContext = Substitute.For<IExecutionContext>();
            _tokenHandler = Substitute.For<ITokenHandler>();
            _sinkHandler = Substitute.For<ISinkHandler>();
            _serviceScope = Substitute.For<IServiceScope>();
            _serviceProvider = Substitute.For<IServiceProvider>();
            _sinkTask = Substitute.For<ISinkTask>();

            _connector = new Connector(_logger, _serviceScopeFactory, _sinkHandlerProvider, _configurationProvider,
                _executionContext, _tokenHandler);
        }

        [Fact]
        public async Task Execute_SimpleSuccess()
        {
            const string connector = "connector";
            var restartsConfig = new RestartsConfig();
            _configurationProvider.GetRestartsConfig().Returns(restartsConfig);
            var token = new CancellationTokenSource();
            var pts = new PauseTokenSource();
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = connector};
            _configurationProvider.GetConnectorConfig(Arg.Any<string>()).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);

            await _connector.Execute(connector, token);

            _configurationProvider.Received().GetRestartsConfig();
            _executionContext.Received().Add(connector, Arg.Any<int>());
            _configurationProvider.Received().GetConnectorConfig(connector);
            _sinkHandlerProvider.Received().GetSinkHandler(connector);
            await _sinkHandler.Received().Startup(connector);
            _executionContext.Received().Pause(connector, Arg.Any<int>());
            _executionContext.Received().Clear(connector);
            _executionContext.Received().Start(connector);
            _executionContext.Received().Stop(connector);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.Received().Execute(connector, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(connector);
            _logger.Received().Debug("Starting tasks.", Arg.Any<object>());
            _logger.Received().Debug("Starting task.", Arg.Any<object>());
            _logger.Received().Debug("Task will be stopped.");
            _logger.Received().Debug("Shutting down the connector.");
            _logger.DidNotReceive().Warning("Unable to load and terminating the task.");
            _logger.DidNotReceive().Error("Task is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Error("Connector is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Debug("Attempting to restart the Connector.", Arg.Any<object>());
            _logger.DidNotReceive().Info("Restart attempts exhausted the threshold for the Connector.", Arg.Any<object>());
            
        }
        
        [Fact]
        public async Task Execute_WhenSinkHandlerNotSetup()
        {
            const string connector = "connector";
            var restartsConfig = new RestartsConfig();
            _configurationProvider.GetRestartsConfig().Returns(restartsConfig);
            var token = new CancellationTokenSource();
            var pts = new PauseTokenSource();
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = connector};
            _configurationProvider.GetConnectorConfig(Arg.Any<string>()).Returns(connectorConfig);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);

            await _connector.Execute(connector, token);

            _configurationProvider.Received().GetRestartsConfig();
            _executionContext.Received().Add(connector, Arg.Any<int>());
            _configurationProvider.Received().GetConnectorConfig(connector);
            _sinkHandlerProvider.Received().GetSinkHandler(connector);
            await _sinkHandler.DidNotReceive().Startup(connector);
            _executionContext.Received().Pause(connector, Arg.Any<int>());
            _executionContext.Received().Clear(connector);
            _executionContext.Received().Start(connector);
            _executionContext.Received().Stop(connector);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.Received().Execute(connector, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.DidNotReceive().Cleanup(connector);
            _logger.Received().Debug("Starting tasks.", Arg.Any<object>());
            _logger.Received().Debug("Starting task.", Arg.Any<object>());
            _logger.Received().Debug("Task will be stopped.");
            _logger.Received().Debug("Shutting down the connector.");
            _logger.DidNotReceive().Warning("Unable to load and terminating the task.");
            _logger.DidNotReceive().Error("Task is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Error("Connector is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Debug("Attempting to restart the Connector.", Arg.Any<object>());
            _logger.DidNotReceive().Info("Restart attempts exhausted the threshold for the Connector.", Arg.Any<object>());
            
        }
        
        public async Task Execute_EnsureConnectorPausedAtStartup()
        {
            const string connector = "connector";
            var restartsConfig = new RestartsConfig();
            _configurationProvider.GetRestartsConfig().Returns(restartsConfig);
            var token = new CancellationTokenSource();
            var pts = new PauseTokenSource();
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = connector, Paused = true};
            _configurationProvider.GetConnectorConfig(Arg.Any<string>()).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);

            await _connector.Execute(connector, token);

            _configurationProvider.Received().GetRestartsConfig();
            _executionContext.Received().Add(connector, Arg.Any<int>());
            _configurationProvider.Received().GetConnectorConfig(connector);
            _sinkHandlerProvider.Received().GetSinkHandler(connector);
            await _sinkHandler.Received().Startup(connector);
            _executionContext.Received().Pause(connector, Arg.Any<int>());
            _executionContext.Received().Clear(connector);
            _executionContext.Received().Start(connector);
            _executionContext.Received().Stop(connector);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.Received().Execute(connector, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(connector);
            _logger.Received().Debug("Starting tasks.", Arg.Any<object>());
            _logger.Received().Debug("Starting task.", Arg.Any<object>());
            _logger.Received().Debug("Task will be stopped.");
            _logger.Received().Debug("Shutting down the connector.");
            _logger.DidNotReceive().Warning("Unable to load and terminating the task.");
            _logger.DidNotReceive().Error("Task is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Error("Connector is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Debug("Attempting to restart the Connector.", Arg.Any<object>());
            _logger.DidNotReceive().Info("Restart attempts exhausted the threshold for the Connector.", Arg.Any<object>());
            
        }
        
        [Fact]
        public async Task Execute_WhenSinkTaskIsNotAvailable()
        {
            const string connector = "connector";
            var restartsConfig = new RestartsConfig();
            _configurationProvider.GetRestartsConfig().Returns(restartsConfig);
            var token = new CancellationTokenSource();
            var pts = new PauseTokenSource();
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = connector};
            _configurationProvider.GetConnectorConfig(Arg.Any<string>()).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);

            await _connector.Execute(connector, token);

            _configurationProvider.Received().GetRestartsConfig();
            _executionContext.Received().Add(connector, Arg.Any<int>());
            _configurationProvider.Received().GetConnectorConfig(connector);
            _sinkHandlerProvider.Received().GetSinkHandler(connector);
            await _sinkHandler.Received().Startup(connector);
            _executionContext.Received().Pause(connector, Arg.Any<int>());
            _executionContext.Received().Clear(connector);
            _executionContext.Received().Start(connector);
            _executionContext.Received().Stop(connector);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.DidNotReceive().Execute(connector, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(connector);
            _logger.Received().Debug("Starting tasks.", Arg.Any<object>());
            _logger.DidNotReceive().Debug("Starting task.", Arg.Any<object>());
            _logger.DidNotReceive().Debug("Task will be stopped.");
            _logger.Received().Debug("Shutting down the connector.");
            _logger.Received().Warning("Unable to load and terminating the task.");
            _logger.DidNotReceive().Error("Task is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Error("Connector is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Debug("Attempting to restart the Connector.", Arg.Any<object>());
            _logger.DidNotReceive().Info("Restart attempts exhausted the threshold for the Connector.", Arg.Any<object>());
            
        }
        
        [Fact]
        public async Task Execute_WhenSinkTaskIsFaulted()
        {
            const string connector = "connector";
            var restartsConfig = new RestartsConfig();
            _configurationProvider.GetRestartsConfig().Returns(restartsConfig);
            var token = new CancellationTokenSource();
            var pts = new PauseTokenSource();
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = connector};
            _configurationProvider.GetConnectorConfig(Arg.Any<string>()).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);
            _sinkTask.Execute(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationTokenSource>())
                .Returns(Task.FromException(new Exception()));
            
            await _connector.Execute(connector, token);

            _configurationProvider.Received().GetRestartsConfig();
            _executionContext.Received().Add(connector, Arg.Any<int>());
            _configurationProvider.Received().GetConnectorConfig(connector);
            _sinkHandlerProvider.Received().GetSinkHandler(connector);
            await _sinkHandler.Received().Startup(connector);
            _executionContext.Received().Pause(connector, Arg.Any<int>());
            _executionContext.Received().Clear(connector);
            _executionContext.Received().Start(connector);
            _executionContext.Received().Stop(connector);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.Received().Execute(connector, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(connector);
            _logger.Received().Debug("Starting tasks.", Arg.Any<object>());
            _logger.Received().Debug("Starting task.", Arg.Any<object>());
            _logger.Received().Debug("Task will be stopped.");
            _logger.Received().Debug("Shutting down the connector.");
            _logger.DidNotReceive().Warning("Unable to load and terminating the task.");
            _logger.Received().Error("Task is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Error("Connector is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Debug("Attempting to restart the Connector.", Arg.Any<object>());
            _logger.DidNotReceive().Info("Restart attempts exhausted the threshold for the Connector.", Arg.Any<object>());
            
        }

        /*
        
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
        */
    }
}