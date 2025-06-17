namespace Kafka.Connect.MySql;

public interface IMySqlClientProvider
{
    IMySqlClient GetMySqlClient(string connector, int taskId = -1);
}

public class MySqlClientProvider(IEnumerable<IMySqlClient> mySqlClients) : IMySqlClientProvider
{
    public IMySqlClient GetMySqlClient(string connector, int taskId = -1)
    {
        return taskId == -1
            ? mySqlClients.FirstOrDefault(p => p.ApplicationName.StartsWith(connector))
            : mySqlClients.SingleOrDefault(p => p.ApplicationName == $"{connector}-{taskId:00}");
    }
}
