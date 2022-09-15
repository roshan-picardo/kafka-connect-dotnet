using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Processors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Processors
{
    public class JsonTypeOverriderTests
    {
        private readonly JsonTypeOverrider _jsonTypeOverrider;
        private readonly IRecordFlattener _recordFlattener;
        private readonly ILogger<JsonTypeOverrider> _logger;
        private readonly IOptions<List<ConnectorConfig<IList<string>>>> _options;


        public JsonTypeOverriderTests()
        {
            _recordFlattener = Substitute.For<IRecordFlattener>();
            _logger = Substitute.For<MockLogger<JsonTypeOverrider>>();
            _options = Substitute.For<IOptions<List<ConnectorConfig<IList<string>>>>>();
            _jsonTypeOverrider = new JsonTypeOverrider(_recordFlattener, _logger, _options, Substitute.For<IOptions<ConnectorConfig<IList<string>>>>());
        }
        
        [Theory]
        [InlineData(new []{ "json.no-convert:{\"field\":\"some-value\"}" }, new string[0], new string [0], false, null, "wrong-connector-name")]
        [InlineData(new []{ "json.no-convert:{\"field\":\"some-value\"}" }, new string[0], new string [0], false, null, "connector-name", "Kafka.Connect.Processors.WhitelistFieldProjector")]
        [InlineData(new []{ "json.convert:null" }, new[]{"json.convert"}, new string [0])]
        [InlineData(new []{ "json.convert:int:100" }, new[]{"json.convert"}, new string [0])]
        [InlineData(new []{ "json.convert:invalid_json" }, new[]{"json.convert"}, new string [0])]
        [InlineData(new []{ "json.convert:{\"field\":\"some-value\"" }, new[]{"json.convert"}, new string [0])]
        [InlineData(new []{ "json.convert:\"field\":\"some-value\"}" }, new[]{"json.convert"}, new string [0])]
        [InlineData(new []{ "json.convert:[\"field\":\"some-value\"" }, new[]{"json.convert"}, new string [0])]
        [InlineData(new []{ "json.convert:\"field\":\"some-value\"]" }, new[]{"json.convert"}, new string [0])]
        [InlineData(new []{ "json.convert:  {\"field\":\"some-value\", invalid:json }" }, new[]{"json.convert"}, new string [0], true)]
        [InlineData(new []{ "json.convert:[\"field\":\"some-value\", invalid:json ] " }, new[]{"json.convert"}, new string [0], true)]
        [InlineData(new []{ "json.convert:{\"field\":\"some-value\"}" }, new[]{"json.convert"}, new[] {"{\"field\":\"some-value\"}"})]
        [InlineData(new []{ "json.convert:[{\"field\":\"some-value\"}]" }, new[]{"json.convert"}, new[] {"[{\"field\":\"some-value\"}]"})]
        [InlineData(new []{ "json.convert:{\"field\":\"some-value\"}" }, new[]{"json.convert"}, new[] {"{\"field\":\"some-value\"}"}, false, "object")]
        [InlineData(new []{ "json.convert:[{\"field\":\"some-value\"}]" }, new[]{"json.convert"}, new[] {"[{\"field\":\"some-value\"}]"}, false, "array")]
        [InlineData(new []{ "json.convert:{\"field\":\"some-value\"}", "json.no-convert:{\"field2\":\"ignore-value\"}" }, new[]{"json.convert"}, new[] {"{\"field\":\"some-value\"}"})]
        [InlineData(new []{ "json.convert.one:{\"field1\":\"some-value\"}", "json.convert.two:[{\"field2\":\"some-value\"}]", "json.convert.three.again:{\"field3\":\"some-value\"}", "json.no-convert:{\"field2\":\"ignore-value\"}" }, new[]{"json.convert.*"}, new[] {"{\"field1\":\"some-value\"}", "[{\"field2\":\"some-value\"}]", "{\"field3\":\"some-value\"}"})]
        [InlineData(new []{ "json.one.convert:{\"field1\":\"some-value\"}", "json.two.convert:[{\"field2\":\"some-value\"}]", "json.three.skip:{\"field3\":\"some-value\"}", "json.four.ignore:{\"field2\":\"ignore-value\"}" }, new[]{"json.*.convert"}, new[] {"{\"field1\":\"some-value\"}", "[{\"field2\":\"some-value\"}]"})]
        [InlineData(new []{ "one.convert.json:{\"field1\":\"some-value\"}", "two.convert.json:[{\"field2\":\"some-value\"}]", "three.skip.json:{\"field3\":\"some-value\"}", "four.ignore.this:{\"field2\":\"ignore-value\"}" }, new[]{"*.convert.json"}, new[] {"{\"field1\":\"some-value\"}", "[{\"field2\":\"some-value\"}]"})]
        [InlineData(new []{ "json.one.convert.first.converted:{\"field1\":\"some-value\"}", "json.two.convert.second.converted:[{\"field2\":\"some-value\"}]", "json.three.ignored.third.converted:{\"field3\":\"some-value\"}", "json.four.convert.fourth.missed:{\"field4\":\"ignore-value\"}" }, new[]{"json.*.convert.*.converted"}, new[] {"{\"field1\":\"some-value\"}", "[{\"field2\":\"some-value\"}]"})]
        [InlineData(new []{ "json.list[0].item:{\"field1\":\"some-value\"}", "json.list[1].item:[{\"field2\":\"some-value\"}]", "json.something.else:{\"field2\":\"ignore-value\"}" }, new[]{"json.list[1].item"}, new[] {"[{\"field2\":\"some-value\"}]"})]
        [InlineData(new []{ "json.list[0].item:{\"field1\":\"some-value\"}", "json.list[1].item:[{\"field2\":\"some-value\"}]", "json.something.else:{\"field2\":\"ignore-value\"}" }, new[]{"json.list[0].item"}, new[] {"{\"field1\":\"some-value\"}"})]
        [InlineData(new []{ "json.list[0].item:{\"field1\":\"some-value\"}", "json.list[1].item:[{\"field2\":\"some-value\"}]", "json.something.else:{\"field2\":\"ignore-value\"}" }, new[]{"json.list[*].item"}, new[] {"{\"field1\":\"some-value\"}", "[{\"field2\":\"some-value\"}]"})]
        [InlineData(new []{ "json.list[0]:{\"field1\":\"some-value\"}", "json.list[1]:[{\"field2\":\"some-value\"}]", "json.something.else:{\"field2\":\"ignore-value\"}" }, new[]{"json.list[*]"}, new[] {"{\"field1\":\"some-value\"}", "[{\"field2\":\"some-value\"}]"})]
        [InlineData(new []{ "json.list[0].one.item:{\"field1\":\"some-value\"}", "json.list[1].two.item:[{\"field2\":\"some-value\"}]", "json.list[2].three.ignore:{\"field3\":\"ignore-value\"}" }, new[]{"json.list[*].*.item"}, new[] {"{\"field1\":\"some-value\"}", "[{\"field2\":\"some-value\"}]"})]
        [InlineData(new []{ "json.list[0].one.array[0].first.item:{\"field1\":\"some-value\"}", "json.list[1].two.array[0].second.item:[{\"field2\":\"some-value\"}]", "json.list[2].three.array[0].ignore:{\"field3\":\"ignore-value\"}" }, new[]{"json.list[*].*.array[*].*.item"}, new[] {"{\"field1\":\"some-value\"}", "[{\"field2\":\"some-value\"}]"})]
        [InlineData(new []{ "json.list[0].item:{\"field1\":\"some-value\"}", "json.list[1].child.item:[{\"field2\":\"some-value\"}]", "json.something.else:{\"field3\":\"ignore-value\"}" }, new[]{"*"}, new[] {"{\"field1\":\"some-value\"}", "[{\"field2\":\"some-value\"}]", "{\"field3\":\"ignore-value\"}"})]
        public async Task Apply_Tests(string[] data, string[] settings,  string[] expected, bool log = false, string jsonFlattened = "none", string connector = "connector-name",  string processor = "Kafka.Connect.Processors.JsonTypeOverrider")
        {
            object GetValue(string s)
            {
                return s.Split(':')[1] switch
                {
                    "int" => int.Parse(s.Split(':')[2]),
                    "null" => null,
                    _ => string.Join(':', s.Split(':')[1..])
                };
            }

            switch (jsonFlattened)
            {
                case "object":
                    _recordFlattener.Flatten(Arg.Any<JToken>())
                        .Returns(new Dictionary<string, object>(){{"field", "some-value" }});
                    break;
                case "array":
                    _recordFlattener.Flatten(Arg.Any<JToken>())
                        .Returns(new Dictionary<string, object>(){{"[0].field", "some-value" }, {"[1].field", "some-value" }});
                    break;
            }

            foreach (var prefix in new[] {"key.", "value.", ""})
            {
                var options = new List<ConnectorConfig<IList<string>>>
                {
                    new()
                    {
                        Name = connector,
                        Processors = new List<ProcessorConfig<IList<string>>>()
                            {new() {Name = processor, Settings = settings.Select(x => $"{prefix}{x}").ToList()}}
                    }
                };

                var flattened = data.ToDictionary(
                    x => prefix == "" ? $"value.{x.Split(':')[0]}" : $"{prefix}{x.Split(':')[0]}",
                    GetValue);

                var expectedJsons = expected.Select(JToken.Parse).ToList();
                var actualJsons = new List<JToken>();
                
                _recordFlattener
                    .When(x => x.Flatten(Arg.Any<JToken>()))
                    .Do(x => actualJsons.Add(x.Arg<JToken>()));
                
                _options.Value.Returns(options);
                var (skip, actual) =
                    await _jsonTypeOverrider.Apply(new Dictionary<string, object>(flattened), "connector-name");
                Assert.False(skip);
                _recordFlattener.Received(expected.Length).Flatten(Arg.Any<JToken>());
                _logger.Received(log ? 1 : 0).Log(LogLevel.Warning, Arg.Any<Exception>(), "{@Log}", new { Message = $"Error while parsing JSON for key: {flattened.Keys.First()}."});

                for (var i = 0; i < expectedJsons.Count; i++)
                {
                    Assert.True(JToken.DeepEquals(expectedJsons[i], actualJsons[i]));
                }
                
                switch (jsonFlattened)
                {
                    case "object":
                        Assert.Contains( $"{(prefix == "" ? "value." : prefix)}json.convert.field", actual.Keys);
                        break;
                    case "array":
                        Assert.Contains($"{(prefix == "" ? "value." : prefix)}json.convert[0].field", actual.Keys);
                        Assert.Contains($"{(prefix == "" ? "value." : prefix)}json.convert[1].field", actual.Keys);
                        break;
                }

                _logger.ClearReceivedCalls();
                _recordFlattener.ClearReceivedCalls();
            }
        }
    }
}