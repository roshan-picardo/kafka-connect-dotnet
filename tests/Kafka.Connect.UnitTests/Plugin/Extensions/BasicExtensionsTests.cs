using System;
using System.Text;
using Kafka.Connect.Plugin.Extensions;
using Xunit;

namespace UnitTests.Kafka.Connect.Plugin.Extensions;

public class BasicExtensionsTests
{
    [Fact]
    public void Serialize_WhenObjectProvided_ReturnsUtf8JsonBytes()
    {
        var payload = new SamplePayload { Id = 42, Name = "alpha" };

        var bytes = ByteConvert.Serialize(payload);
        var json = Encoding.UTF8.GetString(bytes);

        Assert.Contains("\"Id\":42", json);
        Assert.Contains("\"Name\":\"alpha\"", json);
    }

    [Fact]
    public void ToUnixMicroseconds_WhenUtcDateProvided_ReturnsExpectedValue()
    {
        var dateTime = DateTime.UnixEpoch.AddTicks(12345670);

        var result = dateTime.ToUnixMicroseconds();

        Assert.Equal(1234567L, result);
    }

    [Fact]
    public void IsGeneric_WhenTypeMatchesExactly_ReturnsTrue()
    {
        object value = new DerivedType();

        Assert.True(value.Is<DerivedType>());
        Assert.False(value.Is<BaseType>());
    }

    [Fact]
    public void IsByFullName_WhenNameMatchesExactly_ReturnsExpectedValue()
    {
        object value = new DerivedType();

        Assert.True(value.Is(typeof(DerivedType).FullName!));
        Assert.False(value.Is(typeof(BaseType).FullName!));
        Assert.False(((object)null).Is(typeof(DerivedType).FullName!));
    }

    private sealed class SamplePayload
    {
        public int Id { get; init; }
        public string Name { get; init; }
    }

    private class BaseType;
    private sealed class DerivedType : BaseType;
}