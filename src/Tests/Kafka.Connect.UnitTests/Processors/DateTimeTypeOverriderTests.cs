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

public class DateTimeTypeOverriderTests
{
    private readonly DateTimeTypeOverrider _dateTimeTypeOverrider;
    private readonly IConfigurationProvider _configurationProvider;

    public DateTimeTypeOverriderTests()
    {
        _configurationProvider = Substitute.For<IConfigurationProvider>();
        _dateTimeTypeOverrider = new DateTimeTypeOverrider(Substitute.For<ILogger<DateTimeTypeOverrider>>(), _configurationProvider);
    }
        
    [Theory]
    [InlineData(new []{ "date.convert:2022-10-10" }, new []{"date.convert:"}, new string[0], "wrong-connector-name")]
    [InlineData(new []{ "date.convert:2022-10-10" }, new []{"date.convert:"}, new string[0], "connector-name", "Kafka.Connect.Processors.WhitelistFieldProjector")]
    [InlineData(new []{ "date.convert:2022-10-10" }, new []{"date.convert:"}, new []{ "date.convert" })]
    [InlineData(new []{ "date.convert:2022-13-10" }, new []{"date.convert:yyyy-dd-mm"}, new []{ "date.convert" })]
    [InlineData(new []{ "date.convert:" }, new []{"date.convert:yyyy-dd-mm"}, new string[0])]
    [InlineData(new []{ "date.convert:100" }, new []{"date.convert:yyyy-dd-mm"}, new string[0])]
    [InlineData(new []{ "date.convert:not-a-date" }, new []{"date.convert:yyyy-dd-mm"}, new string[0])]
    [InlineData(new []{ "date.convert:not-a-date" }, new []{"date.convert:"}, new string[0])]
    [InlineData(new []{ "date.convert.one:2022-10-8", "date.convert.two:2022-10-8", "date.skip.three:2022-10-8"  }, new []{"date.convert.*:"}, new []{ "date.convert.one", "date.convert.two" })]
    [InlineData(new []{ "date.one.convert:2022-10-8", "date.two.convert:2022-10-8", "date.three.skip:2022-10-8"  }, new []{"date.*.convert:"}, new []{ "date.one.convert", "date.two.convert" })]
    [InlineData(new []{ "one.date.convert:2022-10-8", "two.date.convert:2022-10-8", "three.date.skip:2022-10-8"  }, new []{"*.date.convert:"}, new []{ "one.date.convert", "two.date.convert" })]
    [InlineData(new []{ "simple.list[0].item:2022-10-8", "simple.list[1].item:2022-10-8" }, new []{"simple.list[1].item:"}, new []{"simple.list[1].item"})]
    [InlineData(new []{ "simple.list[0].item:2022-10-8", "simple.list[1].item:2022-10-8" }, new []{"simple.list[*].item:"}, new []{"simple.list[0].item", "simple.list[1].item"})]
    [InlineData(new []{ "simple.list[0]:2022-10-8", "simple.list[1]:2022-10-8" }, new []{"simple.list[*]:"}, new []{"simple.list[0]", "simple.list[1]"})]
    [InlineData(new []{ "simple.list[0].one.keep:2022-10-8", "simple.list[1].two.convert:2022-10-8",  "simple.list[2].three.convert:2022-10-8"  }, new []{"simple.list[*].*.convert:"}, new []{"simple.list[1].two.convert", "simple.list[2].three.convert"})]
    [InlineData(new []{ "simple.list[0].one.keep:2022-10-8", "simple.list[1].two.array[0].child.convert:2022-10-8",  "simple.list[2].three.array[0].another.convert:2022-10-8"  }, new []{"simple.list[*].*.array[*].*.convert:"}, new []{"simple.list[1].two.array[0].child.convert", "simple.list[2].three.array[0].another.convert"})]
    [InlineData(new []{ "simple.list[0].one.keep:2022-10-8", "simple.list[1].two.array[0].child.convert:2022-10-8",  "simple.list[2].three.array[0].another.convert:2022-10-8"  }, new []{"simple.list[1].*.array[*].*.convert:"}, new []{"simple.list[1].two.array[0].child.convert"})]
    public async Task Apply_Tests(string[] data, string[] settings,  string[] expected, string connector = "connector-name", string processor = "Kafka.Connect.Processors.DateTimeTypeOverrider")
    {
        _configurationProvider.GetProcessorSettings<IDictionary<string, string>>(connector, processor).Returns(
            settings.ToDictionary(s => s.Split(':')[0], s => s.Split(':')[1]));
                
        var flattened =
            data.ToDictionary(x => x.Split(':')[0], v => v.Split(':')[1] == "100" ? 100 : (object)v.Split(':')[1]);

        var actual =
            await _dateTimeTypeOverrider.Apply( "connector-name", new ConnectMessage<IDictionary<string, object>>()
            {
                Key = new Dictionary<string, object>(),
                Value = flattened
            });
        Assert.False(actual.Skip);

        foreach (var (key, value) in actual.Value)
        {
            if (expected.Select(x => x.Split(':')[0]).Contains(key))
            {
                Assert.IsType<DateTime>(value);
            }
            else
            {
                Assert.IsNotType<DateTime>(value);
            }
        }
    }

    /*

    [Fact]
    public async Task Apply_WhenKey_InOptions()
    {
        var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
            new Dictionary<string, object>
            {{"key1", "2020-12-10"}, {"key2", "InValid Date"}}, 
            "new[] { \"key1\"}");
        Assert.False(skip);
        Assert.IsType<DateTime>(flattened["key1"]);
        Assert.IsType<string>(flattened["key2"]);
    }
    
    [Fact]
    public async Task Apply_WhenKey_InOptions_WildCard_Elements()
    {
        var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
            new Dictionary<string, object>
                {{"key.sample1", "2020-12-10"}, {"key.sample2", "2020-12-10"}}, 
            @"new[] { ""key.*""}, 
            null");
        Assert.False(skip);
        Assert.IsType<DateTime>(flattened["key.sample1"]);
        Assert.IsType<DateTime>(flattened["key.sample2"]);
    }
    
    [Fact]
    public async Task Apply_WhenKey_InOptions_WildCard_Arrays()
    {
        var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
            new Dictionary<string, object>
                {{"key.array[0]", "2020-12-10"}, {"key.array[1]", "2020-12-15"}}, 
            @"new[] { ""key.array[*]""}, 
            null");
        Assert.False(skip);
        Assert.IsType<DateTime>(flattened["key.array[0]"]);
        Assert.IsType<DateTime>(flattened["key.array[1]"]);
    }
    
    [Fact]
    public async Task Apply_WhenKey_InOptions_WildCard_ArraysAndElements()
    {
        var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
            new Dictionary<string, object>
                {{"key.array[0].item.first", "2020-12-10"}, {"key.array[1].item.second", "2020-12-13"}}, 
            @"new[] { ""key.array[*].item.*""}, 
            null");
        Assert.False(skip);
        Assert.IsType<DateTime>(flattened["key.array[0].item.first"]);
        Assert.IsType<DateTime>(flattened["key.array[1].item.second"]);
    }
    
    [Fact]
    public async Task Apply_WhenKey_InMaps_WithFormat()
    {
        var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
            new Dictionary<string, object>
            {{"key1", "12-2012-10"}, {"key2", "10-12-2020"}},
             @"new Dictionary<string, string>()
            {
                {""key1"", ""MM-yyyy-dd""},
                {""Key2"", ""dd/MM/yyyy""}
            }");
        Assert.False(skip);
        Assert.IsType<DateTime>(flattened["key1"]);
        Assert.IsType<string>(flattened["key2"]);
    }
    
    [Fact]
    public async Task Apply_WhenKey_InMaps_WildCard_Elements_Format()
    {
        var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
            new Dictionary<string, object>
                {{"key.sample1", "12-2020-10"}, {"key.sample2", "12-2020-10"}}, 
            @"maps: new Dictionary<string, string>()
            {
                {""key*"", ""MM-yyyy-dd""}
            }");
        Assert.False(skip);
        Assert.IsType<DateTime>(flattened["key.sample1"]);
        Assert.IsType<DateTime>(flattened["key.sample2"]);
    }
    
    [Fact]
    public async Task Apply_WhenKey_InMaps_WildCard_Arrays_Format()
    {
        var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
            new Dictionary<string, object>
                {{"key.array[0]", "12-2020-10"}, {"key.array[1]", "12-2020-10"}}, 
            @"maps: new Dictionary<string, string>()
            {
                {""key.array[*]"", ""MM-yyyy-dd""}
            }");
        Assert.False(skip);
        Assert.IsType<DateTime>(flattened["key.array[0]"]);
        Assert.IsType<DateTime>(flattened["key.array[1]"]);
    }
    
    [Fact]
    public async Task Apply_WhenKey_InMaps_WildCard_ArraysAndElements_Format()
    {
        var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
            new Dictionary<string, object>
                {{"key.array[0].item.first", "12-2020-10"}, {"key.array[1].item.second", "12-2020-10"}}, 
            @"maps: new Dictionary<string, string>()
            {
                {""key.array[*].item.*"", ""MM-yyyy-dd""}
            }");
        Assert.False(skip);
        Assert.IsType<DateTime>(flattened["key.array[0].item.first"]);
        Assert.IsType<DateTime>(flattened["key.array[1].item.second"]);
    }
    
    [Fact]
    public async Task Apply_Ignore_WhenValue_IsNull()
    {
        var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
            new Dictionary<string, object>
                {{"key1", null}},
            @"maps: new Dictionary<string, string>()
            {
                {""key1"", ""MM-yyyy-dd""}
            }");
        Assert.False(skip);
        Assert.Null(flattened["key1"]);
    }
    
    [Fact]
    public async Task Apply_Ignore_WhenValue_IsNotString()
    {
        var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
            new Dictionary<string, object>
                {{"key1", 500}},
            @"maps: new Dictionary<string, string>()
            {
                {""key1"", ""MM-yyyy-dd""}
            }");
        Assert.False(skip);
        Assert.IsType<int>(flattened["key1"]);
    }
    
    [Fact]
    public async Task Apply_WhenKey_InMapsAndOptions_MergeKeys()
    {
        var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
            new Dictionary<string, object>
                {{"key1", "12-2012-10"}, {"key2", "2020-12-25"}},
            @"maps: new Dictionary<string, string>()
            {
                {""key1"", ""MM-yyyy-dd""}
            },
            options:new List<string>() { ""key2""}");
        Assert.False(skip);
        Assert.IsType<DateTime>(flattened["key1"]);
        Assert.IsType<DateTime>(flattened["key2"]);
    }
    
    */
        
}