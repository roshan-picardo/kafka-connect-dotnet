using System.Collections;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class TestCaseBuilder : IEnumerable<object[]>
{
    public IEnumerator<object[]> GetEnumerator()
    {
        var testConfig = TestConfig.Get();
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var jsonNode = JsonNode.Parse(File.ReadAllText($"{testConfig.RootFolder.TrimEnd('/')}/{testConfig.ConfigFile}"));
        if (jsonNode == null) yield break;
        
        foreach (var node in jsonNode.AsArray())
        {
            var config = node.Deserialize<TestCaseConfig>(options);
            if (config == null) continue;

            var testFiles = new List<string>();

            if (config.Files?.Any() == true)
            {
                foreach (var file in config.Files)
                {
                    var filePath = $"{testConfig.RootFolder.TrimEnd('/')}/{file.TrimStart('/')}";
                    if (File.Exists(filePath))
                    {
                        testFiles.Add(filePath);
                    }
                }
            }
            else if (!string.IsNullOrEmpty(config.Folder))
            {
                var folderPath = $"{testConfig.RootFolder.TrimEnd('/')}/{config.Folder.TrimStart('/')}";
                if (Directory.Exists(folderPath))
                {
                    testFiles = Directory.GetFiles(folderPath, "*.json")
                        .OrderBy(Path.GetFileName)
                        .ToList();
                }
            }

            SchemaRecord? schema = null;
            if (!string.IsNullOrEmpty(config.Schema))
            {
                var schemaPath = $"{testConfig.RootFolder.TrimEnd('/')}/{config.Schema.TrimStart('/')}";
                if (File.Exists(schemaPath))
                {
                    var schemaNode = JsonNode.Parse(File.ReadAllText(schemaPath))?.AsObject();
                    var valueNode = schemaNode?["Value"];
                    if (valueNode != null)
                    {
                        schema = new SchemaRecord(schemaNode?["Key"], valueNode);
                    }
                }
            }

            if (schema == null) continue;

            foreach (var testFile in testFiles)
            {
                if (string.IsNullOrEmpty(testFile) || !File.Exists(testFile)) continue;

                var testData = JsonSerializer.Deserialize<TestCase>(File.ReadAllText(testFile), options);
                if (testData == null) continue;

                yield return
                [
                    testData with { Records = testData.Records ?? [] }
                ];
            }
        }
    }
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

public class TestConfig
{
    public string RootFolder { get; set; } = "data";
    public string ConfigFile { get; set; } = "mongo-test-config.json";

    public static TestConfig Get()
    {
        return new TestConfig
        {
            RootFolder = Path.Combine(Directory.GetCurrentDirectory(), "data"),
            ConfigFile = "mongo-test-config.json"
        };
    }
}