using IntegrationTests.Kafka.Connect.Infrastructure;
using IBM.Data.Db2;
using System.Text.Json;
using System.Text.Json.Nodes;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect;

[Collection("Integration Tests")]
public class TestRunnerDb2(TestFixture fixture, ITestOutputHelper output) : BaseTestRunner(fixture, output)
{
    private readonly TestFixture _fixture = fixture;
    private const string Target = "Db2";

    private DB2Connection GetDb2Connection(string? databaseName = null)
    {
        var connectionString = _fixture.Configuration.GetServiceEndpoint("Db2")!;
        if (!string.IsNullOrEmpty(databaseName))
        {
            connectionString = ReplaceDatabase(connectionString, databaseName);
        }

        return new DB2Connection(connectionString);
    }
    
    [Theory, TestPriority(2)]
    [MemberData(nameof(TestCases), Target)]
    public async Task Execute(TestCase testCase) => await Run(testCase, Target);

    protected override async Task Insert(Dictionary<string, string> properties, TestCaseRecord record)
    {
        await using var connection = GetDb2Connection(properties["database"]);
        await connection.OpenAsync();

        var fields = record.Value!.AsObject().ToDictionary(kv => kv.Key, kv => kv.Value);
        var columns = string.Join(", ", fields.Keys.Select(QuoteIdentifier));
        var values = string.Join(", ", fields.Values.Select(FormatValue));
        var sql = $"INSERT INTO {properties["schema"]}.{properties["table"]} ({columns}) VALUES ({values})";

        await using var command = new DB2Command(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    protected override async Task Update(Dictionary<string, string> properties, TestCaseRecord record)
    {
        await using var connection = GetDb2Connection(properties["database"]);
        await connection.OpenAsync();

        var keyFields = record.Key!.AsObject().ToDictionary(kv => kv.Key, kv => kv.Value);
        var valueFields = record.Value!.AsObject().ToDictionary(kv => kv.Key, kv => kv.Value);
        var setClauses = string.Join(", ", valueFields.Select(field => $"{QuoteIdentifier(field.Key)} = {FormatValue(field.Value)}"));
        var whereConditions = string.Join(" AND ", keyFields.Select(field => $"{QuoteIdentifier(field.Key)} = {FormatValue(field.Value)}"));

        var sql = $"UPDATE {properties["schema"]}.{properties["table"]} SET {setClauses} WHERE {whereConditions}";

        using var command = new DB2Command(sql, connection);

        await command.ExecuteNonQueryAsync();
    }

    protected override async Task Delete(Dictionary<string, string> properties, TestCaseRecord record)
    {
        await using var connection = GetDb2Connection(properties["database"]);
        await connection.OpenAsync();

        var keyFields = record.Key!.AsObject().ToDictionary(kv => kv.Key, kv => kv.Value);
        var whereConditions = string.Join(" AND ", keyFields.Select(field => $"{QuoteIdentifier(field.Key)} = {FormatValue(field.Value)}"));

        var sql = $"DELETE FROM {properties["schema"]}.{properties["table"]} WHERE {whereConditions}";

        using var command = new DB2Command(sql, connection);

        await command.ExecuteNonQueryAsync();
    }

    protected override async Task Setup(Dictionary<string, string> properties)
    {
        await using var connection = GetDb2Connection(properties["database"]);
        await connection.OpenAsync();
        await using var command = new DB2Command(properties["setup"], connection);
        await command.ExecuteNonQueryAsync();
        await connection.CloseAsync();
    }

    protected override async Task Cleanup(Dictionary<string, string> properties)
    {
        await using var connection = GetDb2Connection(properties["database"]);
        await connection.OpenAsync();
        await using var command = new DB2Command(properties["cleanup"], connection);
        await command.ExecuteNonQueryAsync();
        await connection.CloseAsync();
    }   

    protected override async Task<JsonNode?> Search(Dictionary<string, string> properties, TestCaseRecord record)
    {
        await using var connection = GetDb2Connection(properties["database"]);
        await connection.OpenAsync();

        var keyFields = record.Key!.AsObject().ToDictionary(kv => kv.Key, kv => kv.Value);
        var whereConditions = string.Join(" AND ", keyFields.Select(field => $"{QuoteIdentifier(field.Key)} = {FormatValue(field.Value)}"));

        var sql = $"SELECT * FROM {properties["schema"]}.{properties["table"]} WHERE {whereConditions}";

        await using var command = new DB2Command(sql, connection);

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

    private static string ReplaceDatabase(string connectionString, string databaseName)
    {
        var segments = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries).ToList();
        var databaseSegmentIndex = segments.FindIndex(segment => segment.StartsWith("Database=", StringComparison.OrdinalIgnoreCase));
        if (databaseSegmentIndex >= 0)
        {
            segments[databaseSegmentIndex] = $"Database={databaseName}";
        }
        else
        {
            segments.Add($"Database={databaseName}");
        }

        return string.Join(';', segments) + ";";
    }

    private static string QuoteIdentifier(string identifier) => $"\"{identifier}\"";

    private static string FormatValue(JsonNode? node)
    {
        if (node == null)
        {
            return "NULL";
        }

        return node switch
        {
            JsonValue value when value.TryGetValue(out string? stringValue) => $"'{stringValue?.Replace("'", "''")}'",
            JsonValue value when value.TryGetValue(out int intValue) => $"'{intValue}'",
            JsonValue value when value.TryGetValue(out long longValue) => $"'{longValue}'",
            JsonValue value when value.TryGetValue(out decimal decimalValue) => $"'{decimalValue}'",
            JsonValue value when value.TryGetValue(out double doubleValue) => $"'{doubleValue}'",
            JsonValue value when value.TryGetValue(out bool boolValue) => boolValue ? "1" : "0",
            _ => $"'{node.ToJsonString().Replace("'", "''")}'"
        };
    }
}