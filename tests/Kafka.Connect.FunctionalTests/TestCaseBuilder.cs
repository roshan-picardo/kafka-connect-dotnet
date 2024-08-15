using System.Collections;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Kafka.Connect.FunctionalTests
{
    public class TestCaseBuilder : IEnumerable<object[]> 
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            var initConfig = InitConfig.Get();
            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

            foreach (var node in JsonNode.Parse(File.ReadAllText($"{initConfig.RootFolder.TrimEnd('/')}/{initConfig.ConfigFile}")).AsArray())
            {
                var config = node.Deserialize<Config>(options);
                IList<string> files = new List<string>();
                if (!string.IsNullOrEmpty(config.Folder) && Directory.Exists($"{initConfig.RootFolder.TrimEnd('/')}/{config.Folder.TrimStart('/')}"))
                {
                    files = Directory.GetFiles($"{initConfig.RootFolder.TrimEnd('/')}/{config.Folder.TrimStart('/')}", "*.json");
                }
                else
                {
                    if (config.Files.Any())
                    {
                        foreach (var file in config.Files)
                        {
                            if (File.Exists(file))
                            {
                                files.Add(file);
                            }
                            else if(File.Exists($"{initConfig.RootFolder.TrimEnd('/')}/{file.TrimStart('/')}"))
                            {
                                files.Add($"{initConfig.RootFolder.TrimEnd('/')}/{file.TrimStart('/')}");
                            }
                        }
                    }
                }

                Record schema = null;
                if (File.Exists($"{initConfig.RootFolder.TrimEnd('/')}/{config.Schema.TrimStart('/')}"))
                {
                    var schemaNode =
                        JsonNode.Parse(
                            File.ReadAllText($"{initConfig.RootFolder.TrimEnd('/')}/{config.Schema.TrimStart('/')}"))?.AsObject();
                    schema = new Record(schemaNode?["Key"], schemaNode?["Value"]);
                }

                foreach (var dataFile in files)
                {
                    if (string.IsNullOrEmpty(dataFile) || !File.Exists(dataFile)) continue;
                    var data =  JsonSerializer.Deserialize<TestData>(File.ReadAllText(dataFile), options);
                    if (data != null)
                    {
                        yield return new object[]
                        {
                            new TestCase(data.Title ?? dataFile?[dataFile.LastIndexOf('/')..] ?? "", config.Topic, 
                                schema, data.Records)
                        };
                    }
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        
    }
}
