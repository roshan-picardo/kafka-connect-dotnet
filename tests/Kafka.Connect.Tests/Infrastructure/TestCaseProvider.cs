using System.Text.Json;
using System.Text.Json.Nodes;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public static class TestCaseProvider
{
    private const string RootFolder = "data";
    private const string ConfigFile = "test-config.json";
    public static IEnumerable<object[]> GetTestCases(string target = "")
    {
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var jsonNode = JsonNode.Parse(File.ReadAllText($"{RootFolder.TrimEnd('/')}/{ConfigFile}"));
        if (jsonNode == null) yield break;
        
        foreach (var node in jsonNode.AsArray())
        {
            var config = node.Deserialize<TestCaseConfig>(options);
            if (config == null || config.Skip) continue;

            if (!string.IsNullOrEmpty(target) &&
                !string.IsNullOrEmpty(config.Target) &&
                !config.Target.Equals(target, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var testFiles = new List<string>();

            if (config.Files?.Any() == true)
            {
                foreach (var file in config.Files)
                {
                    var filePath = $"{RootFolder.TrimEnd('/')}/{file.TrimStart('/')}";
                    if (File.Exists(filePath))
                    {
                        testFiles.Add(filePath);
                    }
                }
            }
            else if (!string.IsNullOrEmpty(config.Folder))
            {
                var folderPath = $"{RootFolder.TrimEnd('/')}/{config.Folder.TrimStart('/')}";
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
                var schemaPath = $"{RootFolder.TrimEnd('/')}/{config.Schema.TrimStart('/')}";
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

                yield return [testData];
            }
        }
    }
}