using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Processors;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Processors
{
    public class FieldRenamerTests
    {
        private readonly IConfigurationProvider _configurationProvider;
        private readonly FieldRenamer _fieldRenamer;
        public FieldRenamerTests()
        {
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _fieldRenamer = new FieldRenamer(_configurationProvider);
        }
        
        
        
        [Theory]
        [InlineData(new []{ "simple.rename" }, new []{ "simple.rename:simple.new-name" }, new []{"simple.rename"}, "wrong-connector-name")]
        [InlineData(new []{ "simple.rename" }, new []{ "simple.rename:simple.new-name" }, new []{"simple.rename"}, "connector-name", "Kafka.Connect.Processors.BlacklistFieldProjector")]
        [InlineData(new []{ "simple.rename" }, new []{ "simple.rename:simple.new-name" }, new []{"simple.new-name"})]
        [InlineData(new []{ "simple.rename", "simple.do-not" }, new []{"simple.rename:simple.new-name"}, new []{"simple.new-name", "simple.do-not"})]
        [InlineData(new []{ "simple.do-not", "simple.rename.one", "simple.rename.two", "simple.rename.three.what" }, new []{"simple.rename.*:simple.new-name.*"}, new []{"simple.do-not", "simple.new-name.one", "simple.new-name.two", "simple.new-name.three.what"})]
        [InlineData(new []{ "simple.do-not", "simple.one.rename", "simple.two.rename" }, new []{"simple.*.rename:simple.*.new-name"}, new []{"simple.do-not", "simple.one.new-name", "simple.two.new-name"})]
        [InlineData(new []{ "simple.do-not", "one.simple.rename", "two.simple.rename" }, new []{"*.simple.rename:*.simple.new-name"}, new []{ "simple.do-not", "one.simple.new-name", "two.simple.new-name" })]
        [InlineData(new []{ "simple.one.do-not.two.do-not", "simple.one.rename-one.two.rename-two", "simple.three.rename-one.four.rename-two", "simple.one.rename-one.two.do-not" }, new []{"simple.*.rename-one.*.rename-two:simple.*.new-name-first.*.new-name-second"}, new []{ "simple.one.do-not.two.do-not", "simple.one.new-name-first.two.new-name-second", "simple.three.new-name-first.four.new-name-second", "simple.one.rename-one.two.do-not"})]
        [InlineData(new []{ "simple.list[0].item", "simple.list[1].item" }, new []{"simple.list[1].item:new-list[1].child"}, new []{"simple.list[0].item", "new-list[1].child"})]
        [InlineData(new []{ "simple.list[0].item", "simple.list[1].item" }, new []{"simple.list[*].item:parent.push-down.new-list[*].child"}, new []{"parent.push-down.new-list[0].child", "parent.push-down.new-list[1].child"})]
        [InlineData(new []{ "simple.list[0]", "simple.list[1]" }, new []{"simple.list[*]:renamed.new-list[*]"}, new []{"renamed.new-list[0]", "renamed.new-list[1]"})]
        [InlineData(new []{ "simple.list[0].one.do-not", "simple.list[1].two.rename",  "simple.list[2].three.rename"  }, new []{"simple.list[*].*.rename:simple.list[*].*.new-name"}, new []{"simple.list[0].one.do-not","simple.list[1].two.new-name", "simple.list[2].three.new-name"})]
        [InlineData(new []{ "simple.list[0].one.do-not", "simple.list[1].two.array[0].child.rename",  "simple.list[2].three.array[0].another.rename"  }, new []{"simple.list[*].*.array[*].*.rename:parent.list[*].*.child-list[*].*.new-name"}, new []{"simple.list[0].one.do-not", "parent.list[1].two.child-list[0].child.new-name", "parent.list[2].three.child-list[0].another.new-name"})]
        [InlineData(new []{ "simple.list[0].one.do-not", "simple.list[1].two.array[0].child.rename",  "simple.list[2].three.array[0].another.item"  }, new []{"simple.list[1].*.array[*].*.rename:parent.list[1].*.child-list[*].*.new-name"}, new []{"simple.list[0].one.do-not", "parent.list[1].two.child-list[0].child.new-name", "simple.list[2].three.array[0].another.item"})]
        public async Task Apply_Tests(string[] keys, string[] settings,  string[] expected, string connector = "connector-name", string processor = "Kafka.Connect.Processors.FieldRenamer")
        {
            foreach (var prefix in new[] {"key.", "value.", ""})
            {
                _configurationProvider.GetProcessorSettings<IDictionary<string, string>>(connector, processor).Returns(
                    settings.ToDictionary(s => $"{prefix}{s.Split(':')[0]}", s => $"{prefix}{s.Split(':')[1]}"));

                var flattened = keys.ToDictionary(x => prefix == "" ? $"value.{x}" : $"{prefix}{x}", _ => (object) "");

                var (skip, actual) =
                    await _fieldRenamer.Apply(new Dictionary<string, object>(flattened), "connector-name");
                Assert.False(skip);
                Assert.Equal(actual.Count, expected.Length);
                foreach (var (key, _) in actual)
                {
                    Assert.Contains(key,
                        expected.Select(x => prefix == "" ? $"value.{x.Split(':')[0]}" : $"{prefix}{x.Split(':')[0]}"));
                }
            }
        }

    }
}