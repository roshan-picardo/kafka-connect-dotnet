namespace Kafka.Connect.Postgres.Models;

public class PostgresSinkConfig
{
    public string Database { get; set; }
    public string Host { get; set; }
    public int Port { get; set; } = 5432;
    public string UserId { get; set; }
    public string Password { get; set; }
    public string Schema { get; set; } = "public";
    public string Table { get; set; }
    public FilterConfig Filter { get; set; }

    public string ConnectionString => $"Host={Host};Port={Port};User Id={UserId};Password='{Password}';Database={Database}";
}

public class FilterConfig
{
    public string Condition { get; set; } 
    public string[] Keys { get; set; }
}