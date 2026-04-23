using System;
using Kafka.Connect.MariaDb;
using Kafka.Connect.MariaDb.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MariaDb;

public class MariaDbClientProviderTests
{
    [Fact]
    public void GetMariaDbClient_WhenConfigMissing_ThrowsInvalidOperationException()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider.GetPluginConfig<PluginConfig>("missing").Returns((PluginConfig)null);

        var sut = new MariaDbClientProvider(configurationProvider);

        Assert.Throws<InvalidOperationException>(() => sut.GetMariaDbClient("missing", 1));
    }
}
