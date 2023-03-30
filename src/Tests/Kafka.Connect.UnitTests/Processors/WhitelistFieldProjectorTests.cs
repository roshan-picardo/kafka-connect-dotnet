using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Processors;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Processors
{
    public class WhitelistFieldProjectorTests
    {
        private readonly IConfigurationProvider _configurationProvider;
        private readonly WhitelistFieldProjector _whitelistFieldProjector;

        public WhitelistFieldProjectorTests()
        {
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _whitelistFieldProjector = new WhitelistFieldProjector(Substitute.For<ILogger<WhitelistFieldProjector>>(),_configurationProvider);
        }
        
        [Theory]
        [InlineData(new []{ "simple.remove" }, new string[0], new string[0], "wrong-connector-name")]
        [InlineData(new []{ "simple.remove" }, new string[0], new string[0], "connector-name", "Kafka.Connect.Processors.BlacklistFieldProjector")]
        [InlineData(new []{ "simple.remove" }, new string[0], new string[0])]
        [InlineData(new []{ "simple.remove", "simple.keep" }, new []{"simple.remove"}, new []{"simple.remove"})]
        [InlineData(new []{ "simple.remove", "simple.keep.one", "simple.keep.two", "simple.keep.three.what" }, new []{"simple.keep.*"}, new []{"simple.keep.one", "simple.keep.two", "simple.keep.three.what"})]
        [InlineData(new []{ "simple.remove", "simple.one.keep", "simple.two.keep" }, new []{"simple.*.keep"}, new []{"simple.one.keep", "simple.two.keep"})]
        [InlineData(new []{ "simple.remove", "one.simple.keep", "two.simple.keep" }, new []{"*.simple.keep"}, new []{"one.simple.keep", "two.simple.keep"})]
        [InlineData(new []{ "simple.one.remove.two.remove", "simple.one.keep.two.keep", "simple.three.keep.four.keep", "simple.one.keep.two.remove" }, new []{"simple.*.keep.*.keep"}, new []{"simple.one.keep.two.keep", "simple.three.keep.four.keep"})]
        [InlineData(new []{ "simple.keep", "simple.one.keep", "simple.one.two.keep" }, new []{"*"}, new []{"simple.keep", "simple.one.keep", "simple.one.two.keep"})]
        [InlineData(new []{ "simple.list[0].item", "simple.list[1].item" }, new []{"simple.list[1].item"}, new []{"simple.list[1].item"})]
        [InlineData(new []{ "simple.list[0].item", "simple.list[1].item" }, new []{"simple.list[*].item"}, new []{"simple.list[0].item", "simple.list[1].item"})]
        [InlineData(new []{ "simple.list[0]", "simple.list[1]" }, new []{"simple.list[*]"}, new []{"simple.list[0]", "simple.list[1]"})]
        [InlineData(new []{ "simple.list[0].one.remove", "simple.list[1].two.keep",  "simple.list[2].three.keep"  }, new []{"simple.list[*].*.keep"}, new []{"simple.list[1].two.keep", "simple.list[2].three.keep"})]
        [InlineData(new []{ "simple.list[0].one.remove", "simple.list[1].two.array[0].child.item",  "simple.list[2].three.array[0].another.item"  }, new []{"simple.list[*].*.array[*].*.item"}, new []{"simple.list[1].two.array[0].child.item", "simple.list[2].three.array[0].another.item"})]
        [InlineData(new []{ "simple.list[0].one.remove", "simple.list[1].two.array[0].child.item",  "simple.list[2].three.array[0].another.item"  }, new []{"simple.list[1].*.array[*].*.item"}, new []{"simple.list[1].two.array[0].child.item"})]
        public async Task Apply_Tests(string[] keys, string[] settings,  string[] expectedStays, string connector = "connector-name", string processor = "Kafka.Connect.Processors.WhitelistFieldProjector")
        {
            foreach (var prefix in new[] {"key.", "value.", ""})
            {
                _configurationProvider.GetProcessorSettings<IList<string>>(connector, processor)
                    .Returns(settings.Select(x => $"{prefix}{x}").ToList());

                var flattened = keys.ToDictionary(x => prefix == "" ? $"value.{x}" : $"{prefix}{x}", _ => (object) "");
                var stays = expectedStays.Select(x => prefix == "" ? $"value.{x}" : $"{prefix}{x}").ToArray();

                var (skip, actual) =
                    await _whitelistFieldProjector.Apply(new Dictionary<string, object>(flattened), "connector-name");
                Assert.False(skip);
                Assert.Equal(stays.Length, actual.Count);
                Assert.All(stays, key => Assert.True(actual.ContainsKey(key)));
                Assert.All(flattened.Keys.Except(stays), key => Assert.False(actual.ContainsKey(key)));
            }
        }

    }
}