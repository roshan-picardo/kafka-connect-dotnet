using System;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Postgres;
using Kafka.Connect.Postgres.Models;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Postgres;

public class PostgresClientProviderTests
{
    [Fact]
    public void GetPostgresClient_WhenConfigMissing_ThrowsInvalidOperationException()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider.GetPluginConfig<PluginConfig>("missing").Returns((PluginConfig)null);

        var sut = new PostgresClientProvider(configurationProvider);

        Assert.Throws<InvalidOperationException>(() => sut.GetPostgresClient("missing", 1));
    }
}