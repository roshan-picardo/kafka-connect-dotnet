using Cassandra;

namespace Kafka.Connect.Cassandra;

public interface ICassandraSqlExecutor
{
    Task<RowSet> ExecuteAsync(ISession session, string cql);
    Task<IList<Dictionary<string, object>>> QueryRowsAsync(ISession session, string cql);
}

public class CassandraSqlExecutor : ICassandraSqlExecutor
{
    public async Task<RowSet> ExecuteAsync(ISession session, string cql)
    {
        return await session.ExecuteAsync(new SimpleStatement(cql));
    }

    public async Task<IList<Dictionary<string, object>>> QueryRowsAsync(ISession session, string cql)
    {
        var rowSet = await ExecuteAsync(session, cql);
        return rowSet.Select(row => ReadRow(rowSet, row)).ToList();
    }

    private static Dictionary<string, object> ReadRow(RowSet rowSet, Row row)
    {
        var result = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < rowSet.Columns.Length; i++)
        {
            result[rowSet.Columns[i].Name] = row.GetValue<object>(i)!;
        }

        return result;
    }
}
