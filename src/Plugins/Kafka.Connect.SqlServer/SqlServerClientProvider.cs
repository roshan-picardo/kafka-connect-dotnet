namespace Kafka.Connect.SqlServer;

public interface ISqlServerClientProvider
{
    ISqlServerClient GetSqlServerClient(string connector, int taskId = -1);
}

public class SqlServerClientProvider(IEnumerable<ISqlServerClient> sqlServerClients) : ISqlServerClientProvider
{
    public ISqlServerClient GetSqlServerClient(string connector, int taskId = -1)
    {
        return taskId == -1
            ? sqlServerClients.FirstOrDefault(p => p.ApplicationName.StartsWith(connector))
            : sqlServerClients.SingleOrDefault(p => p.ApplicationName == $"{connector}-{taskId:00}");
    }
}
