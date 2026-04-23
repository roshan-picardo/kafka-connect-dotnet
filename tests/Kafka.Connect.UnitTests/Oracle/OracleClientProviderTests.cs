using System;
using Kafka.Connect.Oracle;
using Kafka.Connect.Oracle.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Oracle;

public class OracleClientProviderTests
{
    [Fact]
    public void GetOracleClient_WhenConfigMissing_ThrowsInvalidOperationException()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider.GetPluginConfig<PluginConfig>("missing").Returns((PluginConfig)null);

        var sut = new OracleClientProvider(configurationProvider);

        Assert.Throws<InvalidOperationException>(() => sut.GetOracleClient("missing", 1));
    }
}
