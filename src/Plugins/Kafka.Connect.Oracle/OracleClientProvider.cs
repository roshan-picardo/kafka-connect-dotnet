namespace Kafka.Connect.Oracle;

public interface IOracleClientProvider
{
    IOracleClient GetOracleClient(string connector, int taskId = -1);
}

public class OracleClientProvider(IEnumerable<IOracleClient> oracleClients) : IOracleClientProvider
{
    public IOracleClient GetOracleClient(string connector, int taskId = -1)
    {
        return taskId == -1
            ? oracleClients.FirstOrDefault(p => p.ApplicationName.StartsWith(connector))
            : oracleClients.SingleOrDefault(p => p.ApplicationName == $"{connector}-{taskId:00}");
    }
}
