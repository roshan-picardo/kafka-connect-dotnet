using System;
using Kafka.Connect.Cassandra;
using Kafka.Connect.Cassandra.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Cassandra;

public class CassandraClientProviderTests
{
    [Fact]
    public void GetCassandraClient_WhenConfigMissing_ThrowsInvalidOperationException()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider.GetPluginConfig<PluginConfig>("missing").Returns((PluginConfig)null);

        var sut = new CassandraClientProvider(configurationProvider);

        Assert.Throws<InvalidOperationException>(() => sut.GetCassandraClient("missing", 1));
    }

    [Fact]
    public void GetCassandraClient_WhenHostsMissing_ThrowsInvalidOperationException()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Keyspace = "ks",
            Hosts = []
        });

        var sut = new CassandraClientProvider(configurationProvider);

        Assert.Throws<InvalidOperationException>(() => sut.GetCassandraClient("c1", 1));
    }
}
