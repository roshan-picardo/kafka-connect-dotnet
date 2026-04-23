using System;
using System.Linq;
using Kafka.Connect.Plugin.Exceptions;
using Xunit;

namespace UnitTests.Kafka.Connect.Plugin.Exceptions;

public class ConnectExceptionsTests
{
    [Fact]
    public void ConnectException_DefaultConstructor_UsesUnknownMessage()
    {
        var exception = new PublicConnectException();

        Assert.Equal("Unknown", exception.Message);
    }

    [Fact]
    public void ConnectDataException_AndConnectRetriableException_PreserveInnerException()
    {
        var inner = new InvalidOperationException("boom");

        var dataException = new ConnectDataException("data", inner);
        var retriableException = new ConnectRetriableException("retry", inner);

        Assert.Equal("data", dataException.Message);
        Assert.Same(inner, dataException.InnerException);
        Assert.Equal("retry", retriableException.Message);
        Assert.Same(inner, retriableException.InnerException);
    }

    [Fact]
    public void ConnectAggregateException_WhenAllRetriableExceptions_ShouldRetryIsTrue()
    {
        var exception = new ConnectAggregateException(
            "aggregate",
            false,
            new ConnectRetriableException("one", new Exception("1")),
            new ConnectRetriableException("two", new Exception("2")));

        Assert.True(exception.ShouldRetry);
        Assert.True(exception.CanRetry);
    }

    [Fact]
    public void ConnectAggregateException_WhenMixedExceptions_SplitsConnectAndNonConnectExceptions()
    {
        var connect = new ConnectDataException("bad-data", new Exception("bad"));
        var other = new InvalidOperationException("other");
        var exception = new ConnectAggregateException("aggregate", false, connect, other);

        Assert.False(exception.ShouldRetry);
        Assert.False(exception.CanRetry);
        Assert.Single(exception.GetConnectExceptions());
        Assert.Single(exception.GetNonConnectExceptions());
        Assert.Equal(2, exception.GetAllExceptions().Count());
        Assert.Contains("Inner Exception #1", exception.ToString());
    }

    [Fact]
    public void ConnectToleranceExceededException_SplitsConnectAndNonConnectExceptions()
    {
        var connect = new ConnectDataException("bad-data", new Exception("bad"));
        var other = new InvalidOperationException("other");
        var exception = new ConnectToleranceExceededException("tolerance", connect, other);

        Assert.Single(exception.GetConnectExceptions());
        Assert.Single(exception.GetNonConnectExceptions());
        Assert.Contains("Inner Exception #1", exception.ToString());
    }

    private sealed class PublicConnectException : ConnectException;
}