using System;
using Kafka.Connect.MySql;
using Kafka.Connect.MySql.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MySql;

public class MySqlClientProviderTests
{
    [Fact]
    public void GetMySqlClient_WhenConfigMissing_ThrowsInvalidOperationException()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider.GetPluginConfig<PluginConfig>("missing").Returns((PluginConfig)null);

        var sut = new MySqlClientProvider(configurationProvider);

        Assert.Throws<InvalidOperationException>(() => sut.GetMySqlClient("missing", 1));
    }
}
