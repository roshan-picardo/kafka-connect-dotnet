using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Kafka.Connect.SqlServer;

public interface ISqlServerSqlExecutor
{
    Task<object> ExecuteScalarAsync(SqlConnection connection, string sql);
    Task<int> ExecuteNonQueryAsync(SqlConnection connection, string sql);
    Task<IList<Dictionary<string, object>>> QueryRowsAsync(SqlConnection connection, string sql);
}

public class SqlServerSqlExecutor : ISqlServerSqlExecutor
{
    public async Task<object> ExecuteScalarAsync(SqlConnection connection, string sql)
    {
        return await new SqlCommand(sql, connection).ExecuteScalarAsync();
    }

    public async Task<int> ExecuteNonQueryAsync(SqlConnection connection, string sql)
    {
        return await new SqlCommand(sql, connection).ExecuteNonQueryAsync();
    }

    public async Task<IList<Dictionary<string, object>>> QueryRowsAsync(SqlConnection connection, string sql)
    {
        var rows = new List<Dictionary<string, object>>();
        await using var reader = await new SqlCommand(sql, connection).ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(ReadRow(reader));
        }

        await reader.CloseAsync();
        return rows;
    }

    private static Dictionary<string, object> ReadRow(DbDataReader reader)
    {
        var row = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < reader.FieldCount; i++)
        {
            row.Add(reader.GetName(i), reader.GetValue(i));
        }

        return row;
    }
}
