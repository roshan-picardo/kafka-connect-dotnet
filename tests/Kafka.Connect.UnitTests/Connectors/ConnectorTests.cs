using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Connectors;

public class ConnectorTests
{
    private readonly ILogger<Connector> _logger = Substitute.For<ILogger<Connector>>();
    private readonly IServiceScopeFactory _scopeFactory = Substitute.For<IServiceScopeFactory>();
    private readonly IConfigurationProvider _configurationProvider = Substitute.For<IConfigurationProvider>();
    private readonly IExecutionContext _executionContext = Substitute.For<IExecutionContext>();

    [Fact]
    public async Task Execute_WhenCancelledAfterFirstRetry_StopsConnector()
    {
        var cts = new CancellationTokenSource();

        _configurationProvider.GetConnectorConfig("orders").Returns(new ConnectorConfig
        {
            Name = "orders",
            Tasks = 1,
            Plugin = new PluginConfig { Type = ConnectorType.Sink }
        });

        var provider = Substitute.For<IServiceProvider>();
        provider.GetService(typeof(ISinkTask)).Returns((object)null);

        var scope = Substitute.For<IServiceScope>();
        scope.ServiceProvider.Returns(provider);
        _scopeFactory.CreateScope().Returns(scope);

        _executionContext
            .Retry("orders")
            .Returns(_ =>
            {
                cts.Cancel();
                return Task.FromResult(false);
            });

        var subject = CreateSubject();

        await subject.Execute("orders", cts);

        _executionContext.Received(1).Initialize("orders", subject);
        Assert.True(subject.IsStopped);
    }

    [Fact]
    public void PauseAndResume_UpdatePauseState()
    {
        var subject = CreateSubject();

        subject.Pause();
        Assert.True(subject.IsPaused);

        subject.Resume(new Dictionary<string, string>());
        SpinWait.SpinUntil(() => !subject.IsPaused, 1000);
        Assert.False(subject.IsPaused);
    }

    private Connector CreateSubject() => new(_logger, _scopeFactory, _configurationProvider, _executionContext);
}
