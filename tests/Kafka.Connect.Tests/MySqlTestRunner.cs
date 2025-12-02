using IntegrationTests.Kafka.Connect.Infrastructure;
using MySql.Data.MySqlClient;
using System.Text.Json;
using System.Text.Json.Nodes;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect;

[Collection("Integration Tests")]
public class MySqlTestRunner(TestFixture fixture, ITestOutputHelper output) : BaseTestRunner(fixture, output)
{
    private readonly TestFixture _fixture = fixture;
    private const string Target = "MySql";

    private MySqlConnection GetMySqlConnection(string? databaseName = null)
    {
        var connectionString = _fixture.Configuration.Shakedown.MySql;
        if (!string.IsNullOrEmpty(databaseName))
        {
            var builder = new MySqlConnectionStringBuilder(connectionString)
            {
                Database = databaseName
            };
            connectionString = builder.ConnectionString;
        }
        return new MySqlConnection(connectionString);
    }
    
    [Theory, TestPriority(2)]
    [MemberData(nameof(TestCases), Target)]
    public async Task Execute(TestCase testCase) => await Run(testCase, Target);

    protected override async Task Insert(Dictionary<string, string> properties, TestCaseRecord record)
    {
        await using var connection = GetMySqlConnection(properties["database"]);
        await connection.OpenAsync();

        var jsonValue = record.Value?.ToJsonString() ?? "{}";
        var jsonDoc = JsonDocument.Parse(jsonValue);
        
        var columns = new List<string>();
        var values = new List<object>();
        var parameters = new List<string>();

        foreach (var property in jsonDoc.RootElement.EnumerateObject())
        {
            columns.Add($"`{property.Name}`");
            parameters.Add($"@{property.Name}");
            values.Add(GetParameterValue(property.Value));
        }

        var sql = $"INSERT INTO `{properties["table"]}` ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)})";

        await using var command = new MySqlCommand(sql, connection);
        for (var i = 0; i < columns.Count; i++)
        {
            command.Parameters.AddWithValue(parameters[i].TrimStart('@'), values[i]);
        }

        await command.ExecuteNonQueryAsync();
    }

    protected override async Task Update(Dictionary<string, string> properties, TestCaseRecord record)
    {
        await using var connection = GetMySqlConnection(properties["database"]);
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
            setClauses.Add($"`{property.Name}` = @set_{property.Name}");
            parameters.Add(($"@set_{property.Name}", GetParameterValue(property.Value)));
        }

        // Build WHERE clause from key
        foreach (var property in keyDoc.RootElement.EnumerateObject())
        {
            whereConditions.Add($"`{property.Name}` = @where_{property.Name}");
            parameters.Add(($"@where_{property.Name}", GetParameterValue(property.Value)));
        }

        var sql = $"UPDATE `{properties["table"]}` SET {string.Join(", ", setClauses)} WHERE {string.Join(" AND ", whereConditions)}";
        
        using var command = new MySqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name.TrimStart('@'), value);
        }

        await command.ExecuteNonQueryAsync();
    }

    protected override async Task Delete(Dictionary<string, string> properties, TestCaseRecord record)
    {
        await using var connection = GetMySqlConnection(properties["database"]);
        await connection.OpenAsync();

        var keyJson = record.Key?.ToJsonString() ?? "{}";
        var keyDoc = JsonDocument.Parse(keyJson);

        var whereConditions = new List<string>();
        var parameters = new List<(string name, object value)>();

        foreach (var property in keyDoc.RootElement.EnumerateObject())
        {
            whereConditions.Add($"`{property.Name}` = @{property.Name}");
            parameters.Add(($"@{property.Name}", GetParameterValue(property.Value)));
        }

        var sql = $"DELETE FROM `{properties["table"]}` WHERE {string.Join(" AND ", whereConditions)}";
        
        using var command = new MySqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name.TrimStart('@'), value);
        }

        await command.ExecuteNonQueryAsync();
    }

    protected override async Task Setup(Dictionary<string, string> properties)
    {
        await using var connection = GetMySqlConnection(properties["database"]);
        await connection.OpenAsync();
        await using var command = new MySqlCommand(properties["setup"], connection);
        await command.ExecuteNonQueryAsync();
        await connection.CloseAsync();
    }

    protected override async Task Cleanup(Dictionary<string, string> properties)
    {
        await using var connection = GetMySqlConnection(properties["database"]);
        await connection.OpenAsync();
        await using var command = new MySqlCommand(properties["cleanup"], connection);
        await command.ExecuteNonQueryAsync();
        await connection.CloseAsync();
    }   

    protected override async Task<JsonNode?> Search(Dictionary<string, string> properties, TestCaseRecord record)
    {
        await using var connection = GetMySqlConnection(properties["database"]);
        await connection.OpenAsync();

        var keyJson = record.Key?.ToJsonString() ?? "{}";
        var keyDoc = JsonDocument.Parse(keyJson);

        var whereConditions = keyDoc.RootElement.EnumerateObject().Select(property => $"`{property.Name}` = '{GetParameterValue(property.Value)}'").ToList();

        var sql = $"SELECT * FROM `{properties["table"]}` WHERE {string.Join(" AND ", whereConditions)}";

        await using var command = new MySqlCommand(sql, connection);

        await using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            var recordData = new Dictionary<string, object?>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                var value = reader.GetValue(i);
                recordData[columnName] = value == DBNull.Value ? null : value;
            }
            return JsonSerializer.SerializeToNode(recordData);
        }
        return null;
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

    private static bool HasColumn(MySqlDataReader reader, string columnName)
    {
        for (var i = 0; i < reader.FieldCount; i++)
        {
            if (reader.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }
}