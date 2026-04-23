using System.Data.Common;
using Oracle.ManagedDataAccess.Client;

namespace Kafka.Connect.Oracle;

public interface IOracleSqlExecutor
{
    Task<object> ExecuteScalarAsync(OracleConnection connection, string sql);
    Task<int> ExecuteNonQueryAsync(OracleConnection connection, string sql);
    Task<List<string>> QuerySingleColumnAsync(OracleConnection connection, string sql);
    Task<IList<Dictionary<string, object>>> QueryRowsAsync(OracleConnection connection, string sql);
}

public class OracleSqlExecutor : IOracleSqlExecutor
{
    public async Task<object> ExecuteScalarAsync(OracleConnection connection, string sql)
    {
        return await new OracleCommand(sql, connection).ExecuteScalarAsync();
    }

    public async Task<int> ExecuteNonQueryAsync(OracleConnection connection, string sql)
    {
        return await new OracleCommand(sql, connection).ExecuteNonQueryAsync();
    }

    public async Task<List<string>> QuerySingleColumnAsync(OracleConnection connection, string sql)
    {
        var values = new List<string>();
        await using var reader = await new OracleCommand(sql, connection).ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    public async Task<IList<Dictionary<string, object>>> QueryRowsAsync(OracleConnection connection, string sql)
    {
        var rows = new List<Dictionary<string, object>>();
        await using var reader = await new OracleCommand(sql, connection).ExecuteReaderAsync();
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
            // Oracle commonly returns unquoted aliases in uppercase; use case-insensitive lookup
            // so callers can access metadata columns predictably without losing original names.
            row.Add(reader.GetName(i), reader.GetValue(i));
        }

        return row;
    }
}
