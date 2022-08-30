using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Processors;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Processors
{
    public class JsonTypeOverriderTests
    {
        private readonly JsonTypeOverrider _jsonTypeOverrider;
        private readonly IRecordFlattener _recordFlattener;

        public JsonTypeOverriderTests()
        {
            _recordFlattener = Substitute.For<IRecordFlattener>();
            _jsonTypeOverrider = new JsonTypeOverrider(_recordFlattener, null, null);
        }

        [Fact]
        public async Task Apply_Options_NotJSonData()
        {
            var (skip, flattened) = await _jsonTypeOverrider.Apply(
                new Dictionary<string, object>
                {
                    {"jsonData", "value1"}
                },
                "new[] {\"jsonData\"}");

            Assert.False(skip);
            Assert.True(flattened.ContainsKey("jsonData"));
            Assert.IsType<string>(flattened["jsonData"]);
            _recordFlattener.DidNotReceive().Flatten(Arg.Any<JObject>());
        }


        [Fact]
        public async Task Apply_Options_InvalidJson()
        {
            var (skip, flattened) = await _jsonTypeOverrider.Apply(
                new Dictionary<string, object>
                {
                    {"jsonData", "{ \"data\": \"missingEndQuote }"}
                },
                "new[] {\"jsonData\"}");

            Assert.False(skip);
            Assert.True(flattened.ContainsKey("jsonData"));
            Assert.IsType<string>(flattened["jsonData"]);
            _recordFlattener.DidNotReceive().Flatten(Arg.Any<JObject>());
        }

        [Fact]
        public async Task Apply_Options_ValidJsonObject()
        {
            var expectedJObject = new JObject(){{"data", "validJson"}};
            
            JObject objectToFlatten = null;

            _recordFlattener.Flatten(Arg.Any<JObject>())
                .Returns(new Dictionary<string, object>() {{"data", "validJson"}});

            _recordFlattener
                .When(x => x.Flatten(Arg.Any<JObject>()))
                .Do(x => objectToFlatten = x.Arg<JObject>());

            var (skip, flattened) = await _jsonTypeOverrider.Apply(
                new Dictionary<string, object>
                {
                    {"jsonData", "{ \"data\": \"validJson\" }"}
                },
                "new[] {\"jsonData\"}");

            Assert.False(skip);
            Assert.True(flattened.ContainsKey("jsonData.data"));
            Assert.Equal(expectedJObject, objectToFlatten);
            _recordFlattener.Received().Flatten(Arg.Any<JObject>());
        }
        
        [Fact]
        public async Task Apply_Options_ValidJsonArray()
        {
            var expectedJObject = new JArray {new JObject {{"data", "validJson"}}, new JObject {{"data", "validJson"}}};


            JArray objectToFlatten = null;

            _recordFlattener.Flatten(Arg.Any<JArray>())
                .Returns(new Dictionary<string, object>() {{"[0].data", "validJson"}, {"[1].data", "validJson"}});

            _recordFlattener
                .When(x => x.Flatten(Arg.Any<JArray>()))
                .Do(x => objectToFlatten = x.Arg<JArray>());

            var (skip, flattened) = await _jsonTypeOverrider.Apply(
                new Dictionary<string, object>
                {
                    {"jsonData", "[{ \"data\": \"validJson\" }, { \"data\": \"validJson\" }]"}
                },
                "new[] {\"jsonData\"}");

            Assert.False(skip);
            Assert.True(flattened.ContainsKey("jsonData[0].data") && flattened.ContainsKey("jsonData[1].data"));
            Assert.Equal(expectedJObject, objectToFlatten);
            _recordFlattener.Received().Flatten(Arg.Any<JArray>());
        }
        
        [Fact]
        public async Task Apply_Options_WhenKeyValue_IsNullOrNotString()
        {
            var (skip, flattened) = await _jsonTypeOverrider.Apply(
                new Dictionary<string, object>
                {
                    {"ValueIsNull", null},
                    {"ValueIsNotString", 1000}
                },
                "new[] {\"KeyNotFound\", \"ValueIsNull\", \"ValueIsNotString\" }");

            Assert.False(skip);
            Assert.False(flattened.ContainsKey("KeyNotFound"));
            Assert.True(flattened.ContainsKey("ValueIsNull"));
            Assert.Null(flattened["ValueIsNull"]);
            Assert.True(flattened.ContainsKey("ValueIsNotString"));
            Assert.IsNotType<string>(flattened["ValueIsNotString"]);
            _recordFlattener.DidNotReceive().Flatten(Arg.Any<JObject>());
        }
    }
}