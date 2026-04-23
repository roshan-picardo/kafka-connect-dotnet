using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Extensions;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace UnitTests.Kafka.Connect.Plugin.Extensions;

public class ConverterExtensionsTests
{
    [Fact]
    public void ToDictionary_WhenJsonContainsNestedObjectsAndArrays_FlattensValues()
    {
        var node = JsonNode.Parse("""
        {
          "customer": { "name": "Ada" },
          "items": [
            { "id": 1 },
            { "id": 2 }
          ],
          "enabled": true
        }
        """)!;

        var result = node.ToDictionary();

        Assert.Equal("Ada", result["customer.name"]);
        Assert.Equal(1, result["items[0].id"]);
        Assert.Equal(2, result["items[1].id"]);
        Assert.Equal(true, result["enabled"]);
    }

    [Fact]
    public void ToDictionary_WhenPrefixAndRemovePrefixProvided_ReturnsTrimmedKeys()
    {
        var node = JsonNode.Parse("""
        {
          "payload": {
            "name": "Ada"
          }
        }
        """)!;

        var result = node["payload"]!.ToDictionary("payload", removePrefix: true);

        Assert.Equal("Ada", result["name"]);
    }

    [Fact]
    public void ToNested_WhenDictionaryContainsArrayPaths_RebuildsNestedShape()
    {
        var flattened = new Dictionary<string, object>
        {
            ["customer.name"] = "Ada",
            ["items[0].id"] = 1,
            ["items[1].id"] = 2,
        };

        var nested = flattened.ToNested();
        var json = JsonSerializer.Serialize(nested);

        Assert.Contains("\"customer\":{\"name\":\"Ada\"}", json);
        Assert.Contains("\"items\":[{\"id\":1},{\"id\":2}]", json);
    }

    [Fact]
    public void ToJson_WhenConfigurationProvided_ProducesJsonNode()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                ["connector:name"] = "orders",
                ["connector:tasks:max"] = "2"
            })
            .Build();

        var result = configuration.ToJson();

        Assert.Equal("orders", result["connector"]!["name"]!.GetValue<string>());
        Assert.Equal("2", result["connector"]!["tasks"]!["max"]!.GetValue<string>());
    }

    [Fact]
    public void FromObjectAndToObject_WhenRoundTripped_PreservesValues()
    {
        var source = new SampleOptions { Name = "plugin-a", Count = 3 };

        var flattened = source.FromObject();
        var result = flattened.ToObject<SampleOptions>();

        Assert.Equal("plugin-a", result.Name);
        Assert.Equal(3, result.Count);
    }

    [Fact]
    public void GetValue_WhenJsonElementContainsKnownPrimitiveTypes_ReturnsTypedValue()
    {
        var root = JsonDocument.Parse("""
        {
          "intValue": 12,
          "boolValue": true,
          "nullValue": null
        }
        """).RootElement;

        Assert.Equal(12, root.GetProperty("intValue").GetValue());
        Assert.Equal(true, root.GetProperty("boolValue").GetValue());
        Assert.Null(root.GetProperty("nullValue").GetValue());
    }

    [Fact]
    public void ToGuid_WhenCalledWithSameInput_ReturnsDeterministicGuid()
    {
        var first = "plugin-key".ToGuid();
        var second = "plugin-key".ToGuid();

        Assert.Equal(first, second);
        Assert.NotEqual(Guid.Empty, first);
    }

    private sealed class SampleOptions
    {
        public string Name { get; init; }
        public int Count { get; init; }
    }
}