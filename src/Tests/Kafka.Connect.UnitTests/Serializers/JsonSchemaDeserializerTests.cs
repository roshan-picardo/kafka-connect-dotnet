using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Serializers;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Serializers
{
    public class JsonSchemaDeserializerTests
    {
        private readonly IAsyncDeserializer<JObject> _deserializer;
        private readonly JsonSchemaDeserializer _jsonSchemaDeserializer;
        
        public JsonSchemaDeserializerTests()
        {
            _deserializer = Substitute.For<IAsyncDeserializer<JObject>>();
            _jsonSchemaDeserializer = new JsonSchemaDeserializer(Substitute.For<ILogger<JsonSchemaDeserializer>>(), _deserializer);
        }
        
        //[Fact]
        public async  Task Deserialize()
        { 
            var expected = new JObject {{"value", new JObject {{"json", "this is a test sample!"}}}}; 
            _deserializer.DeserializeAsync(Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<bool>(),
                Arg.Any<SerializationContext>()).Returns(new JObject {{"json", "this is a test sample!"}});

            var actual = await _jsonSchemaDeserializer.Deserialize(ReadOnlyMemory<byte>.Empty, "", null);
            
            Assert.Equal(expected, actual);
        }
    }
}