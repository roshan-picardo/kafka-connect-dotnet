using System;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Strategies;
using Xunit;

namespace UnitTests.Kafka.Connect.Strategies;

public class SkipStrategyTests
{
    [Fact]
    public async Task BuildModels_ReturnsSkippingAndEmptyModels()
    {
        var strategy = new SkipStrategy();

        var actual = await strategy.BuildModels<string>("orders", new ConnectRecord("topic-a", 2, 11));

        Assert.Equal(Status.Skipping, actual.Status);
        Assert.Empty(actual.Models);
    }

    [Fact]
    public async Task Build_ReturnsSkippingStrategyModelWithRecordCoordinates()
    {
        var strategy = new SkipStrategy();
        var record = new ConnectRecord("topic-a", 2, 11);

        var actual = await strategy.Build<string>("orders", record);

        Assert.Equal(Status.Skipping, actual.Status);
        Assert.Equal("topic-a", actual.Topic);
        Assert.Equal(2, actual.Partition);
        Assert.Equal(11, actual.Offset);
        Assert.Empty(actual.Models);
    }
}
