using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Processors;
using Xunit;

namespace UnitTests.Kafka.Connect.Processors;

public class ProcessorHelperTests
{
    [Fact]
    public void GetMatchingKeys_WhenOptionsNull_ReturnsEmpty()
    {
        var flattened = new Dictionary<string, object> { ["field"] = "value" };

        var actual = ((IEnumerable<string>)null).GetMatchingKeys(flattened);

        Assert.Empty(actual);
    }

    [Fact]
    public void GetMatchingMaps_WhenWildcardValueContainsCaptures_RewritesMatchedKey()
    {
        var maps = new Dictionary<string, string>
        {
            ["simple.*.rename"] = "renamed.*.field"
        };
        var flattened = new Dictionary<string, object>
        {
            ["simple.one.rename"] = "a",
            ["simple.two.rename"] = "b"
        };

        var actual = maps.GetMatchingMaps(flattened);

        Assert.Equal("renamed.one.field", actual["simple.one.rename"]);
        Assert.Equal("renamed.two.field", actual["simple.two.rename"]);
    }

    [Fact]
    public void GetMatchingMaps_WhenKeyOnlyTrue_PreservesConfiguredTargetValue()
    {
        var maps = new Dictionary<string, string>
        {
            ["simple.*.date"] = "yyyy-MM-dd"
        };
        var flattened = new Dictionary<string, object>
        {
            ["simple.one.date"] = "2024-01-01"
        };

        var actual = maps.GetMatchingMaps(flattened, keyOnly: true);

        Assert.Equal("yyyy-MM-dd", actual["simple.one.date"]);
    }

    [Theory]
    [InlineData("name", "value.name")]
    [InlineData("key.name", "key.name")]
    [InlineData("value.name", "value.name")]
    public void Prefix_AddsValuePrefixOnlyWhenMissing(string input, string expected)
    {
        Assert.Equal(expected, input.Prefix());
    }

    [Theory]
    [InlineData("key.name", "name")]
    [InlineData("value.name", "name")]
    [InlineData("plain.name", "plain.name")]
    public void Trim_RemovesKnownPrefixes(string input, string expected)
    {
        Assert.Equal(expected, ProcessorHelper.Trim(input));
    }
}