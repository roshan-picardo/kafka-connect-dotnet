using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Serializers;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Kafka.Connect.UnitTests.Serializers
{
    public class StringDeserializerTests
    {
        private StringDeserializer _stringDeserializer;
        public StringDeserializerTests()
        {
            _stringDeserializer = new StringDeserializer();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Deserialize_EmptyOrNull(bool isNull)
        {
            var expected = new JObject{{"value", null}};
            
            var data = new byte[0];
            var actual = await _stringDeserializer.Deserialize(data, SerializationContext.Empty, isNull);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task Deserialize_LengthLessThan5()
        {
            var expected = new JObject{{"value", null}};
            
            var data = new byte[4] { 01, 12, 45, 33} ;
            
           await  Assert.ThrowsAsync<InvalidDataException>(  async () => await _stringDeserializer.Deserialize(data, SerializationContext.Empty));
        }
        
        
        [Fact]
        public async Task Deserialize_SuccessfulConvert()
        {
            var expected = new JObject {{"value", "this is a test sample!"}};

            var data = new byte[]
                {116, 104, 105, 115, 32,116, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 32, 115, 97, 109, 112, 108, 101, 33};

            var actual = await _stringDeserializer.Deserialize(data, SerializationContext.Empty);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact(Skip = "TBD")]
        public async Task Deserialize_FailedToConvert()
        {
            var expected = new JObject {{"value", "this is a test sample!"}};

            var data = new byte[]
                {116, 104, 105, 115, 32,116, 104, 105, 255, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 32, 115, 97, 109, 112, 108, 101, 33};

            var actual = await _stringDeserializer.Deserialize(data, SerializationContext.Empty);
            
            Assert.Equal(expected, actual);
        }
    }
}