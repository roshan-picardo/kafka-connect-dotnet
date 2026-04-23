using System;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Db2;
using Kafka.Connect.Db2.Models;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Db2;

public class Db2ClientProviderTests
{
    [Fact]
    public void GetDb2Client_WhenConfigMissing_ThrowsInvalidOperationException()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider.GetPluginConfig<PluginConfig>("missing").Returns((PluginConfig)null);

        var sut = new Db2ClientProvider(configurationProvider);

        Assert.Throws<InvalidOperationException>(() => sut.GetDb2Client("missing", 1));
    }
}
