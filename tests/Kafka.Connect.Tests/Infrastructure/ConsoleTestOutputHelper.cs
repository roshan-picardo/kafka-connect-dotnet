using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class ConsoleTestOutputHelper : ITestOutputHelper
{
    public void WriteLine(string message)
    {
    }

    public void WriteLine(string format, params object[] args)
    {
    }
}