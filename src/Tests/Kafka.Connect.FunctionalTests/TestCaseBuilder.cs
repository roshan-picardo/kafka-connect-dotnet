using Newtonsoft.Json;
using System.Collections;

namespace Kafka.Connect.FunctionalTests
{
    public class TestCaseBuilder : IEnumerable<object[]> 
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            var initConfig = InitConfig.Get();
            
            var settings = new JsonSerializerSettings
            {
                DateParseHandling = DateParseHandling.None
            };

            foreach (var config in JsonConvert.DeserializeObject<Config[]>(
                         File.ReadAllText($"{initConfig.RootFolder.TrimEnd('/')}/{initConfig.ConfigFile}"), settings)!)
            {
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

                dynamic? schema = null;
                if (File.Exists($"{initConfig.RootFolder.TrimEnd('/')}/{config.Schema.TrimStart('/')}"))
                {
                    schema = JsonConvert.DeserializeObject<dynamic>(File.ReadAllText($"{initConfig.RootFolder.TrimEnd('/')}/{config.Schema.TrimStart('/')}"));
                }

                foreach (var dataFile in files)
                {
                    if (string.IsNullOrEmpty(dataFile) || !File.Exists(dataFile)) continue;
                    var data = JsonConvert.DeserializeObject<TestData>(File.ReadAllText(dataFile), settings);
                    if (data != null)
                    {
                        yield return new object[]
                        {
                            new TestCase(data.Title ?? dataFile?[dataFile.LastIndexOf('/')..] ?? "", config.Topic, 
                                schema, data.Records,
                                data.Sink)
                        };
                    }
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        
    }
}
