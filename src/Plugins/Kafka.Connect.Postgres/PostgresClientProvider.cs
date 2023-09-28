namespace Kafka.Connect.Postgres;

public interface IPostgresClientProvider
{
    IPostgresClient GetPostgresClient(string connector);
}

public class PostgresClientProvider : IPostgresClientProvider
{
    private readonly IEnumerable<IPostgresClient> _postgresClients;

    public PostgresClientProvider(IEnumerable<IPostgresClient> postgresClients)
    {
        _postgresClients = postgresClients;
    }
    
    public IPostgresClient GetPostgresClient(string connector)
    {
        return _postgresClients.SingleOrDefault(p => p.ApplicationName == connector);
    }
}