using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Connect.FunctionalTests;

public class Runner : IClassFixture<Fixture>, IDisposable
{
    private readonly Fixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public Runner(Fixture fixture, ITestOutputHelper testOutputHelper)
    {
        _fixture = fixture;
        _testOutputHelper = testOutputHelper;
    }
    
    [Theory]
    [ClassData(typeof(TestCaseBuilder))]
    public async Task Execute(TestCase testCase)
    {
        await _fixture.Setup(testCase.Expected);
        await _fixture.Send(testCase.Topic, testCase.Schema, testCase.Messages);
        var (status, reason) = await _fixture.Validate(testCase.Expected);
        await _fixture.Cleanup(testCase.Expected);
        Assert.True(status, reason);
    }
    
    public void Dispose()
    {
    }
}