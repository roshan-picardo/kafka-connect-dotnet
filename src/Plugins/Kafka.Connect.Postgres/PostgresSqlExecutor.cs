using System.Data.Common;
using Npgsql;

namespace Kafka.Connect.Postgres;

public interface IPostgresSqlExecutor
{
    Task<object> ExecuteScalarAsync(NpgsqlConnection connection, string sql);
    Task<int> ExecuteNonQueryAsync(NpgsqlConnection connection, string sql);
    Task<IList<Dictionary<string, object>>> QueryRowsAsync(NpgsqlConnection connection, string sql);
}

public class PostgresSqlExecutor : IPostgresSqlExecutor
{
    public async Task<object> ExecuteScalarAsync(NpgsqlConnection connection, string sql)
    {
        return await new NpgsqlCommand(sql, connection).ExecuteScalarAsync();
    }

    public async Task<int> ExecuteNonQueryAsync(NpgsqlConnection connection, string sql)
    {
        return await new NpgsqlCommand(sql, connection).ExecuteNonQueryAsync();
    }

    public async Task<IList<Dictionary<string, object>>> QueryRowsAsync(NpgsqlConnection connection, string sql)
    {
        var rows = new List<Dictionary<string, object>>();
        await using var reader = await new NpgsqlCommand(sql, connection).ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(ReadRow(reader));
        }

        await reader.CloseAsync();
        return rows;
    }

    private static Dictionary<string, object> ReadRow(DbDataReader reader)
    {
        var row = new Dictionary<string, object>();
        for (var i = 0; i < reader.FieldCount; i++)
        {
            row.Add(reader.GetName(i), reader.GetValue(i));
        }

        return row;
    }
}