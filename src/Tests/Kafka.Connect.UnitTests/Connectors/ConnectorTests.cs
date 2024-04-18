using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
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
        private readonly IConnectHandlerProvider _sinkHandlerProvider;
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
            _sinkHandlerProvider = Substitute.For<IConnectHandlerProvider>();
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
            const string name = "connector";
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = name};
            _configurationProvider.GetConnectorConfig(name).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);

            await _connector.Execute(name, GetCancellationToken());

            _executionContext.Received().Initialize(name, _connector);
            _configurationProvider.Received().GetConnectorConfig(name);
            _sinkHandlerProvider.Received().GetSinkHandler(name);
            await _sinkHandler.Received().Startup(name);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.Received().Execute(name, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(name);
            await _executionContext.DidNotReceive().Retry();
            _logger.Received().Debug("Starting tasks.", Arg.Any<object>());
            _logger.Received().Debug("Starting task.", Arg.Any<object>());
            _logger.Received().Debug("Task will be stopped.");
            _logger.Received().Debug("Shutting down the connector.");
            _logger.DidNotReceive().Warning("Unable to load and terminating the task.");
            _logger.DidNotReceive().Error("Task is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Error("Connector is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Debug("Attempting to restart the Connector.", Arg.Any<object>());
            _logger.DidNotReceive().Info("Restart attempts exhausted the threshold for the Connector.", Arg.Any<object>());
            Assert.True(_connector.IsStopped);
        }
        
        [Fact]
        public async Task Execute_CancelTokenWithin()
        {
            const string name = "connector";
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = name};
            _configurationProvider.GetConnectorConfig(name).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);
            var cts = new CancellationTokenSource();
            _logger.When(l => l.Debug("Task will be stopped.")).Do(_ => cts.Cancel());
            

            await _connector.Execute(name, cts);

            _executionContext.Received().Initialize(name, _connector);
            _configurationProvider.Received().GetConnectorConfig(name);
            _sinkHandlerProvider.Received().GetSinkHandler(name);
            await _sinkHandler.Received().Startup(name);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.Received().Execute(name, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(name);
            await _executionContext.DidNotReceive().Retry();
            _logger.Received().Debug("Starting tasks.", Arg.Any<object>());
            _logger.Received().Debug("Starting task.", Arg.Any<object>());
            _logger.Received().Debug("Task will be stopped.");
            _logger.Received().Debug("Shutting down the connector.");
            _logger.DidNotReceive().Warning("Unable to load and terminating the task.");
            _logger.DidNotReceive().Error("Task is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Error("Connector is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Debug("Attempting to restart the Connector.", Arg.Any<object>());
            _logger.DidNotReceive().Info("Restart attempts exhausted the threshold for the Connector.", Arg.Any<object>());
            Assert.True(_connector.IsStopped);
        }
        
        [Fact]
        public async Task Execute_PauseInBetween()
        {
            const string name = "connector";
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = name};
            _configurationProvider.GetConnectorConfig(name).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);
            _logger.When(l => l.Debug("Task will be stopped.")).Do(_ => _connector.Pause());

            await _connector.Execute(name, GetCancellationToken());

            _executionContext.Received().Initialize(name, _connector);
            _configurationProvider.Received().GetConnectorConfig(name);
            _sinkHandlerProvider.Received().GetSinkHandler(name);
            await _sinkHandler.Received().Startup(name);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.Received().Execute(name, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(name);
            await _executionContext.DidNotReceive().Retry();
            _logger.Received().Debug("Starting tasks.", Arg.Any<object>());
            _logger.Received().Debug("Starting task.", Arg.Any<object>());
            _logger.Received().Debug("Task will be stopped.");
            _logger.Received().Debug("Shutting down the connector.");
            _logger.DidNotReceive().Warning("Unable to load and terminating the task.");
            _logger.DidNotReceive().Error("Task is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Error("Connector is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Debug("Attempting to restart the Connector.", Arg.Any<object>());
            _logger.DidNotReceive().Info("Restart attempts exhausted the threshold for the Connector.", Arg.Any<object>());
            Assert.True(_connector.IsStopped);
        }
        
        [Fact]
        public async Task Execute_FailingTaskWhenAll()
        {
             const string name = "connector";
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = name};
            _configurationProvider.GetConnectorConfig(name).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);
            _executionContext.Retry(Arg.Any<string>()).Returns(false);
            _logger.When(l => l.Debug("Task will be stopped.")).Do(_ => throw new Exception());
            
            await _connector.Execute(name, GetCancellationToken());

            _executionContext.Received().Initialize(name, _connector);
            _configurationProvider.Received().GetConnectorConfig(name);
            _sinkHandlerProvider.Received().GetSinkHandler(name);
            await _sinkHandler.Received().Startup(name);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.Received().Execute(name, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(name);
            await _executionContext.Received().Retry(name);
            _logger.Received().Debug("Starting tasks.", Arg.Any<object>());
            _logger.Received().Debug("Starting task.", Arg.Any<object>());
            _logger.Received().Debug("Task will be stopped.");
            _logger.Received().Debug("Shutting down the connector.");
            _logger.DidNotReceive().Warning("Unable to load and terminating the task.");
            _logger.DidNotReceive().Error("Task is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.Received().Error("Connector is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Debug("Attempting to restart the Connector.", Arg.Any<object>());
            _logger.DidNotReceive().Info("Restart attempts exhausted the threshold for the Connector.", Arg.Any<object>());
        }
        
        [Fact]
        public async Task Execute_WhenSinkHandlerNotSetup()
        {
            const string name = "connector";
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = name};
            _configurationProvider.GetConnectorConfig(name).Returns(connectorConfig);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);
            _executionContext.Retry(Arg.Any<string>()).Returns(false);

            await _connector.Execute(name, GetCancellationToken());

            _executionContext.Received().Initialize(name, _connector);
            _configurationProvider.Received().GetConnectorConfig(name);
            _sinkHandlerProvider.Received().GetSinkHandler(name);
            await _sinkHandler.DidNotReceive().Startup(name);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.Received().Execute(name, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.DidNotReceive().Cleanup(name);
            await _executionContext.Received().Retry(name);
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
        public async Task Execute_EnsureConnectorPausedAtStartup()
        {
            const string name = "connector-pause";
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = name, Paused = true};
            _configurationProvider.GetConnectorConfig(name).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);

            await _connector.Execute(name, GetCancellationToken(1, 2000, () => Assert.True(_connector.IsPaused)));
            
            Assert.False(_connector.IsPaused);

            _configurationProvider.Received().GetConnectorConfig(name);
            _sinkHandlerProvider.Received().GetSinkHandler(name);
            await _sinkHandler.Received().Startup(name);
            _serviceScopeFactory.DidNotReceive().CreateScope();
            _serviceProvider.DidNotReceive().GetService<ISinkTask>();
            await _sinkTask.DidNotReceive().Execute(name, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(name);
            _logger.DidNotReceive().Debug("Starting tasks.", Arg.Any<object>());
            _logger.DidNotReceive().Debug("Starting task.", Arg.Any<object>());
            _logger.DidNotReceive().Debug("Task will be stopped.");
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
            const string name = "connector";
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = name};
            _configurationProvider.GetConnectorConfig(name).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _executionContext.Retry(Arg.Any<string>()).Returns(false);
            
            await _connector.Execute(name, GetCancellationToken());

            _executionContext.Received().Initialize(name, _connector);
            _configurationProvider.Received().GetConnectorConfig(name);
            _sinkHandlerProvider.Received().GetSinkHandler(name);
            await _sinkHandler.Received().Startup(name);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.DidNotReceive().Execute(name, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(name);
            await _executionContext.Received().Retry(name);
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
            const string name = "connector";
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = name};
            _configurationProvider.GetConnectorConfig(name).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);
            _executionContext.Retry(Arg.Any<string>()).Returns(false);
            _sinkTask.Execute(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationTokenSource>())
                .Returns(Task.FromException(new Exception()));
            
            await _connector.Execute(name, GetCancellationToken());

            _executionContext.Received().Initialize(name, _connector);
            _configurationProvider.Received().GetConnectorConfig(name);
            _sinkHandlerProvider.Received().GetSinkHandler(name);
            await _sinkHandler.Received().Startup(name);
            _serviceScopeFactory.Received().CreateScope();
            _serviceProvider.Received().GetService<ISinkTask>();
            await _sinkTask.Received().Execute(name, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(name);
            await _executionContext.Received().Retry(name);
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
        
        [Fact]
        public async Task Execute_WhenSinkTaskIsFaultedAndRetried()
        {
            const string name = "connector";
            var connectorConfig = new ConnectorConfig{MaxTasks = 1, Name = name};
            _configurationProvider.GetConnectorConfig(name).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);
            _executionContext.Retry(Arg.Any<string>()).Returns(true, false);
            _sinkTask.Execute(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationTokenSource>())
                .Returns(Task.FromException(new Exception()));
            
            await _connector.Execute(name, GetCancellationToken(3));

            _executionContext.Received().Initialize(name, _connector);
            _configurationProvider.Received().GetConnectorConfig(name);
            _sinkHandlerProvider.Received().GetSinkHandler(name);
            await _sinkHandler.Received().Startup(name);
            _serviceScopeFactory.Received(2).CreateScope();
            _serviceProvider.Received(2).GetService<ISinkTask>();
            await _sinkTask.Received(2).Execute(name, 1, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(name);
            await _executionContext.Received(2).Retry(name);
            _logger.Received(2).Debug("Starting tasks.", Arg.Any<object>());
            _logger.Received(2).Debug("Starting task.", Arg.Any<object>());
            _logger.Received(2).Debug("Task will be stopped.");
            _logger.Received().Debug("Shutting down the connector.");
            _logger.DidNotReceive().Warning("Unable to load and terminating the task.");
            _logger.Received(2).Error("Task is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Error("Connector is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Debug("Attempting to restart the Connector.", Arg.Any<object>());
            _logger.DidNotReceive().Info("Restart attempts exhausted the threshold for the Connector.", Arg.Any<object>());
        }
        
        [Fact]
        public async Task Execute_SimpleSuccessMultipleTasks()
        {
            const string name = "connector";
            var connectorConfig = new ConnectorConfig{MaxTasks = 5, Name = name};
            _configurationProvider.GetConnectorConfig(name).Returns(connectorConfig);
            _sinkHandlerProvider.GetSinkHandler(Arg.Any<string>()).Returns(_sinkHandler);
            _serviceScopeFactory.CreateScope().Returns(_serviceScope);
            _serviceScope.ServiceProvider.Returns(_serviceProvider);
            _serviceProvider.GetService<ISinkTask>().Returns(_sinkTask);

            await _connector.Execute(name, GetCancellationToken());

            _executionContext.Received().Initialize(name, _connector);
            _configurationProvider.Received().GetConnectorConfig(name);
            _sinkHandlerProvider.Received().GetSinkHandler(name);
            await _sinkHandler.Received().Startup(name);
            _serviceScopeFactory.Received(5).CreateScope();
            _serviceProvider.Received(5).GetService<ISinkTask>();
            for (var i = 1; i <= 5; i++)
                await _sinkTask.Received(1).Execute(name, i, Arg.Any<CancellationTokenSource>());
            await _sinkHandler.Received().Cleanup(name);
            await _executionContext.DidNotReceive().Retry();
            _logger.Received().Debug("Starting tasks.", Arg.Any<object>());
            _logger.Received(5).Debug("Starting task.", Arg.Any<object>());
            _logger.Received(5).Debug("Task will be stopped.");
            _logger.Received().Debug("Shutting down the connector.");
            _logger.DidNotReceive().Warning("Unable to load and terminating the task.");
            _logger.DidNotReceive().Error("Task is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Error("Connector is faulted, and will be terminated.", Arg.Any<Exception>());
            _logger.DidNotReceive().Debug("Attempting to restart the Connector.", Arg.Any<object>());
            _logger.DidNotReceive().Info("Restart attempts exhausted the threshold for the Connector.", Arg.Any<object>());
        }

        [Fact]
        public void Pause_Tests()
        {
            _connector.Pause();
            Assert.True(_connector.IsPaused);
        }
        
        [Fact]
        public void Resume_Tests()
        {
            _connector.Resume(null);
            Assert.False(_connector.IsPaused);
        }
        

        private CancellationTokenSource GetCancellationToken(int loop = 2, int delay = 0, Action assertBeforeCancel = null)
        {
            var cts = new CancellationTokenSource();
            _tokenHandler.When(k => k.DoNothing()).Do(_ =>
            {
                if (--loop == 0)
                {
                    assertBeforeCancel?.Invoke();
                    if(delay == 0) cts.Cancel();
                    else cts.CancelAfter(delay);
                }
            });
            return cts;
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