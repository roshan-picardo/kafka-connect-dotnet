using System;
using Kafka.Connect.MongoDb.Collections;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MongoDb.Collections;

public class MongoClientProviderTests
{
    [Fact]
    public void GetMongoClient_WhenConfigMissing_ThrowsInvalidOperationException()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("missing").Returns((PluginConfig)null);

        var sut = new MongoClientProvider(configProvider);

        Assert.Throws<InvalidOperationException>(() => sut.GetMongoClient("missing", 1));
    }

    [Fact]
    public void GetMongoClient_ReturnsCachedClientPerConnectorTask()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            ConnectionUri = "mongodb://localhost:27017/{2}",
            Username = "u",
            Password = null,
            Database = "db"
        });

        var sut = new MongoClientProvider(configProvider);

        var a = sut.GetMongoClient("c1", 2);
        var b = sut.GetMongoClient("c1", 2);

        Assert.Same(a, b);
    }
}
