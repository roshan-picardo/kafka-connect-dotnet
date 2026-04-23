using System.Data.Common;
using MySql.Data.MySqlClient;

namespace Kafka.Connect.MySql;

public interface IMySqlSqlExecutor
{
    Task<object> ExecuteScalarAsync(MySqlConnection connection, string sql);
    Task<int> ExecuteNonQueryAsync(MySqlConnection connection, string sql);
    Task<List<string>> QuerySingleColumnAsync(MySqlConnection connection, string sql);
    Task<IList<Dictionary<string, object>>> QueryRowsAsync(MySqlConnection connection, string sql);
}

public class MySqlSqlExecutor : IMySqlSqlExecutor
{
    public async Task<object> ExecuteScalarAsync(MySqlConnection connection, string sql)
    {
        return await new MySqlCommand(sql, connection).ExecuteScalarAsync();
    }

    public async Task<int> ExecuteNonQueryAsync(MySqlConnection connection, string sql)
    {
        return await new MySqlCommand(sql, connection).ExecuteNonQueryAsync();
    }

    public async Task<List<string>> QuerySingleColumnAsync(MySqlConnection connection, string sql)
    {
        var values = new List<string>();
        await using var reader = await new MySqlCommand(sql, connection).ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    public async Task<IList<Dictionary<string, object>>> QueryRowsAsync(MySqlConnection connection, string sql)
    {
        var rows = new List<Dictionary<string, object>>();
        await using var reader = await new MySqlCommand(sql, connection).ExecuteReaderAsync();
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
            row.Add(reader.GetName(i).ToLower(), reader.GetValue(i));
        }

        return row;
    }
}
