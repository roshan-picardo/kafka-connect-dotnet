using System;
using System.IO;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Serializers;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Serializers
{
    public class JsonDeserializerTests
    {
        private readonly JsonDeserializer _jsonDeserializer;
        public JsonDeserializerTests()
        {
            _jsonDeserializer = new JsonDeserializer(Substitute.For<ILogger<JsonDeserializer>>());
        }

        [Fact]
        public async Task Deserialize_EmptyOrNull()
        {
            var expected = new JObject{{"value", null}};
            
            var data = Array.Empty<byte>();
            var actual = await _jsonDeserializer.Deserialize(data, "", null);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task Deserialize_LengthLessThan5()
        {
            var data = new byte[] { 01, 12, 45, 33} ;
            
           await  Assert.ThrowsAsync<InvalidDataException>(  async () => await _jsonDeserializer.Deserialize(data, "", null));
        }
        
        
        [Fact]
        public async Task Deserialize_SuccessfulConvert()
        {
            var expected = new JObject {{"value", new JObject {{"json", "this is a test sample!"}}}};

            var data = new byte[]
            {
                123, 34, 106, 115, 111, 110, 34, 32, 58, 34, 116, 104, 105, 115, 32, 105, 115,
                32, 97, 32, 116, 101, 115, 116, 32, 115, 97, 109, 112, 108, 101, 33, 34, 125
            };

            var actual = await _jsonDeserializer.Deserialize(data, "", null);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact(Skip = "TBD")]
        public async Task Deserialize_FailedToConvert()
        {
            var expected = new JObject {{"value", "this is a test sample!"}};

            var data = new byte[]
                {116, 104, 105, 115, 32,116, 104, 105, 255, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 32, 115, 97, 109, 112, 108, 101, 33};

            var actual = await _jsonDeserializer.Deserialize(data, "", null);
            
            Assert.Equal(expected, actual);
        }
    }
}