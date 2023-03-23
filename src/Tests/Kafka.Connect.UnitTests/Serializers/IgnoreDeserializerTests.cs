using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Serializers;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Kafka.Connect.UnitTests.Serializers
{
    public class IgnoreDeserializerTests
    {
        [Fact]
        public async  Task Deserialize()
        {
            var ignoreDeserializer = new IgnoreDeserializer();
            
            var expected = new JObject{{"value", null}};

            var actual = await ignoreDeserializer.Deserialize(ReadOnlyMemory<byte>.Empty, SerializationContext.Empty);
            
            Assert.Equal(expected, actual);

        }
    }
}