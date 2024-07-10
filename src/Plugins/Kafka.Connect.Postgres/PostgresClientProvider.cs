namespace Kafka.Connect.Postgres;

public interface IPostgresClientProvider
{
    IPostgresClient GetPostgresClient(string connector, int taskId = -1);
}

public class PostgresClientProvider(IEnumerable<IPostgresClient> postgresClients) : IPostgresClientProvider
{
    public IPostgresClient GetPostgresClient(string connector, int taskId = -1)
    {
        return taskId == -1
            ? postgresClients.FirstOrDefault(p => p.ApplicationName.StartsWith(connector))
            : postgresClients.SingleOrDefault(p => p.ApplicationName == $"{connector}-{taskId:00}");
    }
}