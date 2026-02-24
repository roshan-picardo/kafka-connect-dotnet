using System.Text.Json.Nodes;
using Xunit;
using Xunit.Abstractions;
using IntegrationTests.Kafka.Connect.Infrastructure;

namespace IntegrationTests.Kafka.Connect;

[Collection("Integration Tests")]
public class HealthTestRunner(TestFixture fixture, ITestOutputHelper output) : BaseTestRunner(fixture, output)
{
    private readonly TestFixture _fixture = fixture;
    private readonly ITestOutputHelper _output = output;
    private const string Target = "Health";

    [Theory, TestPriority(1)]
    [MemberData(nameof(TestCases), Target)]
    public async Task Execute(TestCase testCase)
    {
        switch (testCase.Properties["target"]?.ToLower())
        {
            case "mongo":
                await new MongoTestRunner(_fixture, _output).Execute(testCase);
                break;
            case "postgres":
                await new PostgresTestRunner(_fixture, _output).Execute(testCase);
                break;
            case "sqlserver":
                await new SqlServerTestRunner(_fixture, _output).Execute(testCase);
                break;
            case "mysql":
                await new MySqlTestRunner(_fixture, _output).Execute(testCase);
                break;
            case "mariadb":
                await new MariaDbTestRunner(_fixture, _output).Execute(testCase);
                break;
            case "oracle":
                await new OracleTestRunner(_fixture, _output).Execute(testCase);
                break;
            case "dynamodb":
                await new DynamoDbTestRunner(_fixture, _output).Execute(testCase);
                break;
            default:
                await Run(testCase, Target);
                break;
        }
    }

    protected override Task Setup(Dictionary<string, string> properties) => Task.CompletedTask;

    protected override Task Cleanup(Dictionary<string, string> properties) => Task.CompletedTask;

    protected override Task<JsonNode?> Search(Dictionary<string, string> properties, TestCaseRecord record) => Task.FromResult<JsonNode?>(null);

    protected override Task Insert(Dictionary<string, string> properties, TestCaseRecord record) => Task.CompletedTask;

    protected override Task Update(Dictionary<string, string> properties, TestCaseRecord record) => Task.CompletedTask;

    protected override Task Delete(Dictionary<string, string> properties, TestCaseRecord record) => Task.CompletedTask;
}