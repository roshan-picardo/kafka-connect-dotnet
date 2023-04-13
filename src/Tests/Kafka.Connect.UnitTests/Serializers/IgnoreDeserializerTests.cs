using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Serializers;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Serializers
{
    public class IgnoreDeserializerTests
    {
        [Fact]
        public async  Task Deserialize()
        {
            var ignoreDeserializer = new IgnoreDeserializer(Substitute.For<ILogger<IgnoreDeserializer>>());
            
            var expected = new JObject{{"value", null}};

            var actual = await ignoreDeserializer.Deserialize(ReadOnlyMemory<byte>.Empty, "", new Dictionary<string, byte[]>());
            
            Assert.Equal(expected, actual);

        }
    }
}