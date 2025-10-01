namespace Kafka.Connect.MariaDb;

public interface IMariaDbClientProvider
{
    IMariaDbClient GetMariaDbClient(string connector, int taskId = -1);
}

public class MariaDbClientProvider(IEnumerable<IMariaDbClient> mariaDbClients) : IMariaDbClientProvider
{
    public IMariaDbClient GetMariaDbClient(string connector, int taskId = -1)
    {
        return taskId == -1
            ? mariaDbClients.FirstOrDefault(p => p.ApplicationName.StartsWith(connector))
            : mariaDbClients.SingleOrDefault(p => p.ApplicationName == $"{connector}-{taskId:00}");
    }
}
