using Kafka.Connect.Utilities;
using Xunit;

namespace UnitTests.Kafka.Connect.Utilities;

public class ArgumentsTests
{
    [Fact]
    public void Parse_SupportsFlagsQuotedValuesAndMultipleEntries()
    {
        var args = new[]
        {
            "--name:alpha",
            "--multi:first",
            "--multi:second",
            "--flag",
            "--quoted:\"hello world\"",
            "loose"
        };

        var parsed = Arguments.Parse(args);

        Assert.True(parsed.TryGetValue("name", out var name));
        Assert.Equal(new[] { "alpha" }, name);

        Assert.True(parsed.TryGetValue("multi", out var multi));
        Assert.Equal(new[] { "first", "second" }, multi);

        Assert.True(parsed.TryGetValue("flag", out var flag));
        Assert.Equal(new[] { "true" }, flag);

        Assert.True(parsed.TryGetValue("quoted", out var quoted));
        Assert.Equal(new[] { "hello world" }, quoted);

        Assert.False(parsed.TryGetValue("missing", out _));
    }

    [Fact]
    public void Parse_SetsTrailingSwitchToTrue()
    {
        var parsed = Arguments.Parse(new[] { "--enable" });

        Assert.True(parsed.TryGetValue("enable", out var value));
        Assert.Equal(new[] { "true" }, value);
    }
}
