using Cassandra;
using IntegrationTests.Kafka.Connect.Infrastructure;
using System.Text.Json;
using System.Text.Json.Nodes;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect;

[Collection("Integration Tests")]
public class TestRunnerCassandra(TestFixture fixture, ITestOutputHelper output) : BaseTestRunner(fixture, output)
{
    private readonly TestFixture _fixture = fixture;
    private const string Target = "Cassandra";

    [Theory, TestPriority(2)]
    [MemberData(nameof(TestCases), Target)]
    public async Task Execute(TestCase testCase) => await Run(testCase, Target);

    protected override async Task Insert(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var session = _fixture.GetCassandraSession();
        var payload = record.Value?.ToJsonString() ?? "{}";
        var jsonDoc = JsonDocument.Parse(payload);

        var columns = new List<string>();
        var values = new List<string>();

        foreach (var property in jsonDoc.RootElement.EnumerateObject())
        {
            columns.Add($"\"{property.Name}\"");
            values.Add(GetCqlLiteral(property.Value));
        }

        var cql = $"INSERT INTO {properties["keyspace"]}.{properties["table"]} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", values)});";
        await session.ExecuteAsync(new SimpleStatement(cql));
    }

    protected override async Task Update(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var session = _fixture.GetCassandraSession();
        var keyDoc = JsonDocument.Parse(record.Key?.ToJsonString() ?? "{}");
        var valueDoc = JsonDocument.Parse(record.Value?.ToJsonString() ?? "{}");

        var setClause = string.Join(", ", valueDoc.RootElement.EnumerateObject()
            .Select(property => $"\"{property.Name}\" = {GetCqlLiteral(property.Value)}"));

        var whereClause = string.Join(" AND ", keyDoc.RootElement.EnumerateObject()
            .Select(property => $"\"{property.Name}\" = {GetCqlLiteral(property.Value)}"));

        var cql = $"UPDATE {properties["keyspace"]}.{properties["table"]} SET {setClause} WHERE {whereClause};";
        await session.ExecuteAsync(new SimpleStatement(cql));
    }

    protected override async Task Delete(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var session = _fixture.GetCassandraSession();
        var keyDoc = JsonDocument.Parse(record.Key?.ToJsonString() ?? "{}");

        var whereClause = string.Join(" AND ", keyDoc.RootElement.EnumerateObject()
            .Select(property => $"\"{property.Name}\" = {GetCqlLiteral(property.Value)}"));

        var cql = $"DELETE FROM {properties["keyspace"]}.{properties["table"]} WHERE {whereClause};";
        await session.ExecuteAsync(new SimpleStatement(cql));
    }

    protected override async Task Setup(Dictionary<string, string> properties)
    {
        var session = _fixture.GetCassandraSession();
        await session.ExecuteAsync(new SimpleStatement(properties["setup"]));
    }

    protected override async Task Cleanup(Dictionary<string, string> properties)
    {
        var session = _fixture.GetCassandraSession();
        await session.ExecuteAsync(new SimpleStatement(properties["cleanup"]));
    }

    protected override async Task<JsonNode?> Search(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var session = _fixture.GetCassandraSession();
        var keyDoc = JsonDocument.Parse(record.Key?.ToJsonString() ?? "{}");

        var whereClause = string.Join(" AND ", keyDoc.RootElement.EnumerateObject()
            .Select(property => $"\"{property.Name}\" = {GetCqlLiteral(property.Value)}"));

        var cql = $"SELECT * FROM {properties["keyspace"]}.{properties["table"]} WHERE {whereClause} LIMIT 1;";
        var rows = await session.ExecuteAsync(new SimpleStatement(cql));
        var row = rows.FirstOrDefault();

        if (row == null)
        {
            return null;
        }

        var result = new Dictionary<string, object?>();
        foreach (var column in rows.Columns)
        {
            result[column.Name] = row.GetValue<object>(column.Name);
        }

        return JsonSerializer.SerializeToNode(result);
    }

    private static string GetCqlLiteral(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => $"'{element.GetString()?.Replace("'", "''")}'",
            JsonValueKind.Number => element.ToString(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => "null",
            _ => $"'{element.ToString().Replace("'", "''")}'"
        };
    }
}
