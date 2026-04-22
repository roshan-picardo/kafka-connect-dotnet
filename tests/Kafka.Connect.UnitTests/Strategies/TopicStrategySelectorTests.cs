using System.Collections.Generic;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Strategies;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Strategies;

public class TopicStrategySelectorTests
{
    [Fact]
    public void GetStrategy_WhenSettingsNull_ReturnsNull()
    {
        var selector = new TopicStrategySelector([]);

        var actual = selector.GetStrategy(new ConnectRecord("topic-a", 0, 1), null);

        Assert.Null(actual);
    }

    [Fact]
    public void GetStrategy_WhenTopicMissingFromSettings_ReturnsNull()
    {
        var strategy = Substitute.For<IStrategy>();
        var selector = new TopicStrategySelector([strategy]);

        var actual = selector.GetStrategy(new ConnectRecord("topic-a", 0, 1), new Dictionary<string, string>
        {
            ["topic-b"] = typeof(SkipStrategy).FullName
        });

        Assert.Null(actual);
    }

    [Fact]
    public void GetStrategy_WhenMatchingTypeConfigured_ReturnsMatchingStrategy()
    {
        var strategy = new SkipStrategy();
        var selector = new TopicStrategySelector([strategy]);

        var actual = selector.GetStrategy(new ConnectRecord("topic-a", 0, 1), new Dictionary<string, string>
        {
            ["topic-a"] = typeof(SkipStrategy).FullName
        });

        Assert.Same(strategy, actual);
    }

    [Fact]
    public void GetStrategy_WhenConfiguredTypeMissing_ReturnsNull()
    {
        var strategy = new SkipStrategy();
        var selector = new TopicStrategySelector([strategy]);

        var actual = selector.GetStrategy(new ConnectRecord("topic-a", 0, 1), new Dictionary<string, string>
        {
            ["topic-a"] = typeof(TopicStrategySelectorTests).FullName
        });

        Assert.Null(actual);
    }
}
