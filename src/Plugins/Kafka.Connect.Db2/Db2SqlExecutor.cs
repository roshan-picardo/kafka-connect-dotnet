using System.Data.Common;
using IBM.Data.Db2;

namespace Kafka.Connect.Db2;

public interface IDb2SqlExecutor
{
    Task<object> ExecuteScalarAsync(DB2Connection connection, string sql);
    Task<int> ExecuteNonQueryAsync(DB2Connection connection, string sql);
    Task<IList<Dictionary<string, object>>> QueryRowsAsync(DB2Connection connection, string sql);
}

public class Db2SqlExecutor : IDb2SqlExecutor
{
    public async Task<object> ExecuteScalarAsync(DB2Connection connection, string sql)
    {
        return await new DB2Command(sql, connection).ExecuteScalarAsync();
    }

    public async Task<int> ExecuteNonQueryAsync(DB2Connection connection, string sql)
    {
        return await new DB2Command(sql, connection).ExecuteNonQueryAsync();
    }

    public async Task<IList<Dictionary<string, object>>> QueryRowsAsync(DB2Connection connection, string sql)
    {
        var rows = new List<Dictionary<string, object>>();
        await using var reader = await new DB2Command(sql, connection).ExecuteReaderAsync();
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
