namespace Kafka.Connect.Postgres;

public interface IPostgresClientProvider
{
    IPostgresClient GetPostgresClient(string connector, int taskId);
}

public class PostgresClientProvider : IPostgresClientProvider
{
    private readonly IEnumerable<IPostgresClient> _postgresClients;

    public PostgresClientProvider(IEnumerable<IPostgresClient> postgresClients)
    {
        _postgresClients = postgresClients;
    }
    
    public IPostgresClient GetPostgresClient(string connector, int taskId)
    {
        return _postgresClients.SingleOrDefault(p => p.ApplicationName == $"{connector}-{taskId:00}");
    }
}