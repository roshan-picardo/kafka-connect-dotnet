namespace Kafka.Connect.Postgres.Models;

public class PostgresSinkConfig
{
    public string Database { get; set; }
    public string Host { get; set; }
    public string Username { get; set; }
    public string Password { get; set; }
    public string Table { get; set; }
    public string[] Keys { get; set; }
}