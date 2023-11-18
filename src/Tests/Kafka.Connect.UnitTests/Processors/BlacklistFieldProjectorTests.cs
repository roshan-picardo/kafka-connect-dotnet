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

public class BlacklistFieldProjectorTests
{
    private readonly BlacklistFieldProjector _blacklistFieldProjector;
    private readonly ILogger<BlacklistFieldProjector> _logger;
    private readonly IConfigurationProvider _configurationProvider;

    public BlacklistFieldProjectorTests()
    {
        _configurationProvider = Substitute.For<IConfigurationProvider>();
        _logger = Substitute.For<ILogger<BlacklistFieldProjector>>();
        _blacklistFieldProjector = new BlacklistFieldProjector(_logger, _configurationProvider);
    }

    [Theory]
    [InlineData(new []{ "simple.keep" }, new string[0], new string[0], "wrong-connector-name")]
    [InlineData(new []{ "simple.keep" }, new string[0], new string[0], "connector-name", "Kafka.Connect.Processors.WhitelistFieldProjector")]
    [InlineData(new []{ "simple.keep" }, new string[0], new string[0])]
    [InlineData(new []{ "simple.keep", "simple.remove" }, new []{"simple.remove"}, new []{"simple.remove"})]
    [InlineData(new []{ "simple.keep", "simple.remove.one", "simple.remove.two", "simple.remove.three.what" }, new []{"simple.remove.*"}, new []{"simple.remove.one", "simple.remove.two", "simple.remove.three.what"})]
    [InlineData(new []{ "simple.keep", "simple.one.remove", "simple.two.remove" }, new []{"simple.*.remove"}, new []{"simple.one.remove", "simple.two.remove"})]
    [InlineData(new []{ "simple.keep", "one.simple.remove", "two.simple.remove" }, new []{"*.simple.remove"}, new []{"one.simple.remove", "two.simple.remove"})]
    [InlineData(new []{ "simple.one.keep.two.keep", "simple.one.remove.two.remove", "simple.three.remove.four.remove", "simple.one.remove.two.keep" }, new []{"simple.*.remove.*.remove"}, new []{"simple.one.remove.two.remove", "simple.three.remove.four.remove"})]
    [InlineData(new []{ "simple.remove", "simple.one.remove", "simple.one.two.remove" }, new []{"*"}, new []{"simple.remove", "simple.one.remove", "simple.one.two.remove"})]
    [InlineData(new []{ "simple.list[0].item", "simple.list[1].item" }, new []{"simple.list[1].item"}, new []{"simple.list[1].item"})]
    [InlineData(new []{ "simple.list[0].item", "simple.list[1].item" }, new []{"simple.list[*].item"}, new []{"simple.list[0].item", "simple.list[1].item"})]
    [InlineData(new []{ "simple.list[0]", "simple.list[1]" }, new []{"simple.list[*]"}, new []{"simple.list[0]", "simple.list[1]"})]
    [InlineData(new []{ "simple.list[0].one.keep", "simple.list[1].two.remove",  "simple.list[2].three.remove"  }, new []{"simple.list[*].*.remove"}, new []{"simple.list[1].two.remove", "simple.list[2].three.remove"})]
    [InlineData(new []{ "simple.list[0].one.keep", "simple.list[1].two.array[0].child.item",  "simple.list[2].three.array[0].another.item"  }, new []{"simple.list[*].*.array[*].*.item"}, new []{"simple.list[1].two.array[0].child.item", "simple.list[2].three.array[0].another.item"})]
    [InlineData(new []{ "simple.list[0].one.keep", "simple.list[1].two.array[0].child.item",  "simple.list[2].three.array[0].another.item"  }, new []{"simple.list[1].*.array[*].*.item"}, new []{"simple.list[1].two.array[0].child.item"})]
    public async Task Apply_Tests(string[] keys, string[] settings,  string[] expectedRemoved, string connector = "connector-name", string processor = "Kafka.Connect.Processors.BlacklistFieldProjector")
    {
        _configurationProvider.GetProcessorSettings<IList<string>>(connector, processor)
            .Returns(settings.ToList());

        var flattened = keys.ToDictionary(x => x, _ => (object) "");
        var removed = expectedRemoved.ToArray();

        var actual =
            await _blacklistFieldProjector.Apply("connector-name", new ConnectMessage<IDictionary<string, object>>
            {
                Key = new Dictionary<string, object>(),
                Value = flattened
            });
        Assert.False(actual.Skip);
        Assert.Equal(keys.Length - removed.Length, actual.Value.Count);
        Assert.All(flattened.Keys.Except(removed), key => Assert.True(actual.Value.ContainsKey(key)));
        Assert.All(removed, key => Assert.False(actual.Value.ContainsKey(key)));
    }
}