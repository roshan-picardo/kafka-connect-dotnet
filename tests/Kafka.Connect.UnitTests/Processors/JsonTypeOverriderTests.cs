using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Processors;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Processors;

public class JsonTypeOverriderTests
{
    private readonly JsonTypeOverrider _jsonTypeOverrider;
    private readonly ILogger<JsonTypeOverrider> _logger;
    private readonly IConfigurationProvider _configurationProvider;

    public JsonTypeOverriderTests()
    {
        _logger = Substitute.For<ILogger<JsonTypeOverrider>>();
        _configurationProvider = Substitute.For<IConfigurationProvider>();
        _jsonTypeOverrider = new JsonTypeOverrider(_logger, _configurationProvider);
    }

    [Fact]
    public async Task Apply_WhenJsonObjectConfigured_FlattensObjectFields()
    {
        _configurationProvider.GetProcessorSettings<IList<string>>("connector-name", typeof(JsonTypeOverrider).FullName)
            .Returns(new List<string> { "json.convert" });

        var actual = await _jsonTypeOverrider.Apply("connector-name", new ConnectMessage<IDictionary<string, object>>
        {
            Key = new Dictionary<string, object>(),
            Value = new Dictionary<string, object>
            {
                ["json.convert"] = "{\"field\":\"some-value\",\"count\":3}"
            }
        });

        Assert.False(actual.Skip);
        Assert.False(actual.Flattened.Value.ContainsKey("json.convert"));
        Assert.Equal("some-value", actual.Flattened.Value["json.convert.field"]);
        Assert.Equal(3, Convert.ToInt32(actual.Flattened.Value["json.convert.count"]));
    }

    [Fact]
    public async Task Apply_WhenJsonArrayConfigured_FlattensArrayFields()
    {
        _configurationProvider.GetProcessorSettings<IList<string>>("connector-name", typeof(JsonTypeOverrider).FullName)
            .Returns(new List<string> { "json.convert" });

        var actual = await _jsonTypeOverrider.Apply("connector-name", new ConnectMessage<IDictionary<string, object>>
        {
            Key = new Dictionary<string, object>(),
            Value = new Dictionary<string, object>
            {
                ["json.convert"] = "[{\"field\":\"first\"},{\"field\":\"second\"}]"
            }
        });

        Assert.False(actual.Skip);
        Assert.False(actual.Flattened.Value.ContainsKey("json.convert"));
        Assert.Equal("first", actual.Flattened.Value["json.convert[0].field"]);
        Assert.Equal("second", actual.Flattened.Value["json.convert[1].field"]);
    }

    [Fact]
    public async Task Apply_WhenKeySettingConfigured_TransformsKeyOnly()
    {
        _configurationProvider.GetProcessorSettings<IList<string>>("connector-name", typeof(JsonTypeOverrider).FullName)
            .Returns(new List<string> { "key.json.convert" });

        var actual = await _jsonTypeOverrider.Apply("connector-name", new ConnectMessage<IDictionary<string, object>>
        {
            Key = new Dictionary<string, object>
            {
                ["key.json.convert"] = "{\"field\":\"key-value\"}"
            },
            Value = new Dictionary<string, object>
            {
                ["json.convert"] = "{\"field\":\"value-stays-raw\"}"
            }
        });

        Assert.Equal("key-value", actual.Flattened.Key["key.json.convert.field"]);
        Assert.Equal("{\"field\":\"value-stays-raw\"}", actual.Flattened.Value["json.convert"]);
    }

    [Fact]
    public async Task Apply_WhenJsonInvalid_LogsWarningAndLeavesValueUntouched()
    {
        _configurationProvider.GetProcessorSettings<IList<string>>("connector-name", typeof(JsonTypeOverrider).FullName)
            .Returns(new List<string> { "json.convert" });

        var actual = await _jsonTypeOverrider.Apply("connector-name", new ConnectMessage<IDictionary<string, object>>
        {
            Key = new Dictionary<string, object>(),
            Value = new Dictionary<string, object>
            {
                ["json.convert"] = "{\"field\": invalid }"
            }
        });

        Assert.False(actual.Skip);
        Assert.Equal("{\"field\": invalid }", actual.Flattened.Value["json.convert"]);
        _logger.Received(1).Warning("Error while parsing JSON for key: json.convert.", Arg.Any<Exception>());
    }

    [Fact]
    public async Task Apply_WhenValueIsNotString_IgnoresConfiguredField()
    {
        _configurationProvider.GetProcessorSettings<IList<string>>("connector-name", typeof(JsonTypeOverrider).FullName)
            .Returns(new List<string> { "json.convert" });

        var actual = await _jsonTypeOverrider.Apply("connector-name", new ConnectMessage<IDictionary<string, object>>
        {
            Key = new Dictionary<string, object>(),
            Value = new Dictionary<string, object>
            {
                ["json.convert"] = 123
            }
        });

        Assert.Equal(123, actual.Flattened.Value["json.convert"]);
        _logger.DidNotReceive().Warning(Arg.Any<string>(), Arg.Any<object>(), Arg.Any<Exception>());
    }

    [Fact]
    public async Task Apply_WhenPatternMatchesMultipleFields_FlattensOnlyMatchingEntries()
    {
        _configurationProvider.GetProcessorSettings<IList<string>>("connector-name", typeof(JsonTypeOverrider).FullName)
            .Returns(new List<string> { "json.list[*].item" });

        var actual = await _jsonTypeOverrider.Apply("connector-name", new ConnectMessage<IDictionary<string, object>>
        {
            Key = new Dictionary<string, object>(),
            Value = new Dictionary<string, object>
            {
                ["json.list[0].item"] = "{\"field\":\"first\"}",
                ["json.list[1].item"] = "[{\"field\":\"second\"}]",
                ["json.other"] = "{\"field\":\"untouched\"}"
            }
        });

        Assert.Equal("first", actual.Flattened.Value["json.list[0].item.field"]);
        Assert.Equal("second", actual.Flattened.Value["json.list[1].item[0].field"]);
        Assert.Equal("{\"field\":\"untouched\"}", actual.Flattened.Value["json.other"]);
    }
}