using System;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Serializers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Serializers;

public class StringDeserializerTests
{
    private readonly StringDeserializer _stringDeserializer;
    public StringDeserializerTests()
    {
        _stringDeserializer = new StringDeserializer(Substitute.For<ILogger<StringDeserializer>>());
    }

    [Fact]
    public async Task Deserialize_EmptyOrNull()
    {
        var data =  Array.Empty<byte>() ;
        var actual = await _stringDeserializer.Deserialize(data, "", null);
            
        Assert.Null(actual);
    }
        
    [Fact]
    public async Task Deserialize_LengthLessThan5()
    {
        var data = new byte[] { 01, 12, 45, 33} ;
            
        await  Assert.ThrowsAsync<InvalidDataException>(  async () => await _stringDeserializer.Deserialize(data, "", null));
    }
        
        
    [Fact]
    public async Task Deserialize_SuccessfulConvert()
    {
        var expected = JsonValue.Create("this is a test sample!");

        var data = new byte[]
            {116, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 32, 115, 97, 109, 112, 108, 101, 33};

        var actual = await _stringDeserializer.Deserialize(data, "", null);
            
        Assert.Equal(expected.ToJsonString(), actual.ToJsonString());
    }
        
    [Fact(Skip = "TBD")]
    public async Task Deserialize_FailedToConvert()
    {
        var expected = new JsonObject {{"value", "this is a test sample!"}};

        var data = new byte[]
            {116, 104, 105, 115, 32,116, 104, 105, 255, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 32, 115, 97, 109, 112, 108, 101, 33};

        var actual = await _stringDeserializer.Deserialize(data, "", null);
            
        Assert.Equal(expected, actual);
    }
}