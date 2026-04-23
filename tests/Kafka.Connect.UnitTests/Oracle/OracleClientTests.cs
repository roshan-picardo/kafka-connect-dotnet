using Kafka.Connect.Oracle;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace UnitTests.Kafka.Connect.Oracle;

public class OracleClientTests
{
    [Fact]
    public void Constructor_WithInvalidConnection_ThrowsOracleException()
    {
        var connection = new OracleConnection("Data Source=127.0.0.1/invalid;User Id=u;Password=p;Connection Timeout=1;");

        Assert.Throws<OracleException>(() => new OracleClient("c1", connection));
    }
}
