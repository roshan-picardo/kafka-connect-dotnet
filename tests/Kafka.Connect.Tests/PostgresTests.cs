using IntegrationTests.Kafka.Connect.Infrastructure;
using Npgsql;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect;

[Collection("Integration Tests")]
public class PostgresTests(TestFixture fixture, ITestOutputHelper output) : BaseTests<PostgresProperties>(fixture, output)
{
    private readonly TestFixture _fixture = fixture;

    [Theory, TestPriority(2)]
    [MemberData(nameof(TestCases), "Postgres")]
    public async Task Execute(TestCase<PostgresProperties> testCase) => await ExecuteTest(testCase);

    protected override async Task Insert(PostgresProperties properties, TestCaseRecord record)
    {
        await using var connection = _fixture.GetPostgresConnection(properties.Database);
        await connection.OpenAsync();

        var jsonValue = record.Value?.ToJsonString() ?? "{}";
        var jsonDoc = JsonDocument.Parse(jsonValue);
        
        var columns = new List<string>();
        var values = new List<object>();
        var parameters = new List<string>();

        foreach (var property in jsonDoc.RootElement.EnumerateObject())
        {
            columns.Add($"\"{property.Name}\"");
            parameters.Add($"@{property.Name}");
            values.Add(GetParameterValue(property.Value));
        }

        var sql = $"INSERT INTO {properties.Schema}.\"{properties.Table}\" ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)})";

        await using var command = new NpgsqlCommand(sql, connection);
        for (var i = 0; i < columns.Count; i++)
        {
            command.Parameters.AddWithValue(parameters[i].TrimStart('@'), values[i]);
        }

        await command.ExecuteNonQueryAsync();
    }

    protected override async Task Update(PostgresProperties properties, TestCaseRecord record)
    {
        await using var connection = _fixture.GetPostgresConnection(properties.Database);
        await connection.OpenAsync();

        var keyJson = record.Key?.ToJsonString() ?? "{}";
        var valueJson = record.Value?.ToJsonString() ?? "{}";
        
        var keyDoc = JsonDocument.Parse(keyJson);
        var valueDoc = JsonDocument.Parse(valueJson);

        var setClauses = new List<string>();
        var whereConditions = new List<string>();
        var parameters = new List<(string name, object value)>();

        // Build SET clause from value
        foreach (var property in valueDoc.RootElement.EnumerateObject())
        {
            setClauses.Add($"\"{property.Name}\" = @set_{property.Name}");
            parameters.Add(($"@set_{property.Name}", GetParameterValue(property.Value)));
        }

        // Build WHERE clause from key
        foreach (var property in keyDoc.RootElement.EnumerateObject())
        {
            whereConditions.Add($"\"{property.Name}\" = @where_{property.Name}");
            parameters.Add(($"@where_{property.Name}", GetParameterValue(property.Value)));
        }

        var sql = $"UPDATE {properties.Schema}.\"{properties.Table}\" SET {string.Join(", ", setClauses)} WHERE {string.Join(" AND ", whereConditions)}";
        
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name.TrimStart('@'), value);
        }

        await command.ExecuteNonQueryAsync();
    }

    protected override async Task Delete(PostgresProperties properties, TestCaseRecord record)
    {
        using var connection = _fixture.GetPostgresConnection(properties.Database);
        await connection.OpenAsync();

        var keyJson = record.Key?.ToJsonString() ?? "{}";
        var keyDoc = JsonDocument.Parse(keyJson);

        var whereConditions = new List<string>();
        var parameters = new List<(string name, object value)>();

        foreach (var property in keyDoc.RootElement.EnumerateObject())
        {
            whereConditions.Add($"\"{property.Name}\" = @{property.Name}");
            parameters.Add(($"@{property.Name}", GetParameterValue(property.Value)));
        }

        var sql = $"DELETE FROM {properties.Schema}.\"{properties.Table}\" WHERE {string.Join(" AND ", whereConditions)}";
        
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name.TrimStart('@'), value);
        }

        await command.ExecuteNonQueryAsync();
    }

    protected override async Task Setup(PostgresProperties properties)
    {
        await using var connection = _fixture.GetPostgresConnection(properties.Database);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(properties.Setup, connection);
        await command.ExecuteNonQueryAsync();
        await connection.CloseAsync();
    }

    protected override async Task Cleanup(PostgresProperties properties)
    {
        await using var connection = _fixture.GetPostgresConnection(properties.Database);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(properties.Cleanup, connection);
        await command.ExecuteNonQueryAsync();
        await connection.CloseAsync();
    }

    protected override async Task Search(PostgresProperties properties, TestCaseRecord record)
    {
        await using var connection = _fixture.GetPostgresConnection(properties.Database);
        await connection.OpenAsync();

        var keyJson = record.Key?.ToJsonString() ?? "{}";
        var keyDoc = JsonDocument.Parse(keyJson);

        var whereConditions = new List<string>();
        var parameters = new List<(string name, object value)>();

        foreach (var property in keyDoc.RootElement.EnumerateObject())
        {
            whereConditions.Add($"{property.Name} = @{property.Name}");
            parameters.Add(($"@{property.Name}", GetParameterValue(property.Value)));
        }

        var sql = $"SELECT * FROM {properties.Schema}.{properties.Table} WHERE {string.Join(" AND ", whereConditions)}";

        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name.TrimStart('@'), value);
        }

        await using var reader = await command.ExecuteReaderAsync();
        
        if (record.Value != null)
        {
            // Expecting a record to exist
            Assert.True(await reader.ReadAsync(), "Expected record not found in database");
            
            var expectedJson = record.Value.ToJsonString();
            var expectedDoc = JsonDocument.Parse(expectedJson);
            
            // Verify each expected field
            foreach (var expectedProperty in expectedDoc.RootElement.EnumerateObject())
            {
                var columnName = expectedProperty.Name;
                Assert.True(HasColumn(reader, columnName), $"Column '{columnName}' not found in result");
                
                var actualValue = reader[columnName];
                var expectedValue = GetParameterValue(expectedProperty.Value);
                
                Assert.Equal(expectedValue, actualValue);
            }
        }
        else
        {
            // Expecting no record to exist
            Assert.False(await reader.ReadAsync(), "Expected no record but found one in database");
        }
    }

    private static object GetParameterValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString() ?? string.Empty,
            JsonValueKind.Number => element.TryGetInt32(out var intVal) ? intVal : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => DBNull.Value,
            _ => element.ToString()
        };
    }

    private static bool HasColumn(NpgsqlDataReader reader, string columnName)
    {
        for (var i = 0; i < reader.FieldCount; i++)
        {
            if (reader.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }
}

public record PostgresProperties(string Database, string Schema, string Table, string Setup, string Cleanup) : TargetProperties
{
    public override string ToString()
    {
        return $"Database: {Database}, Schema: {Schema}, Table: {Table}";
    }
}