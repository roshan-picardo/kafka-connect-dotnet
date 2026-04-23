using System;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.SqlServer;
using Kafka.Connect.SqlServer.Models;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.SqlServer;

public class SqlServerClientProviderTests
{
    [Fact]
    public void GetSqlServerClient_WhenConfigMissing_ThrowsInvalidOperationException()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider.GetPluginConfig<PluginConfig>("missing").Returns((PluginConfig)null);

        var sut = new SqlServerClientProvider(configurationProvider);

        Assert.Throws<InvalidOperationException>(() => sut.GetSqlServerClient("missing", 1));
    }
}
