using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;
using IConfigurationProvider = Kafka.Connect.Plugin.Providers.IConfigurationProvider;

namespace UnitTests.Kafka.Connect.Providers;

public class DefaultLogRecordTests
{
    private readonly IConfigurationProvider _configurationProvider = Substitute.For<IConfigurationProvider>();

    [Fact]
    public void Enrich_ReturnsNestedAttributesForKeyExactAndArrayWildcard()
    {
        _configurationProvider.GetLogAttributes<string[]>("orders")
            .Returns(["_key", "field", "workers[*]", "nested.name"]);
        var enricher = new DefaultLogRecord(_configurationProvider);

        var actual = enricher.Enrich(CreateRecord(JsonNode.Parse("""
        {
          "field": "value",
          "workers": ["worker-a", "worker-b"],
          "nested": { "name": "alpha" }
        }
        """)), "orders");

        var json = JsonSerializer.SerializeToNode(actual);
        Assert.Equal("record-key", json?["_key"]?.GetValue<string>());
        Assert.Equal("value", json?["field"]?.GetValue<string>());
        Assert.Equal("alpha", json?["nested"]?["name"]?.GetValue<string>());
        Assert.Equal("worker-a", json?["workers"]?[0]?.GetValue<string>());
        Assert.Equal("worker-b", json?["workers"]?[1]?.GetValue<string>());
    }

    [Fact]
    public void Enrich_SkipsMissingAndNullValues()
    {
        _configurationProvider.GetLogAttributes<string[]>("orders")
                        .Returns(["field", "missing", "items[*]"]);
        var enricher = new DefaultLogRecord(_configurationProvider);

        var actual = enricher.Enrich(CreateRecord(JsonNode.Parse("""
        {
          "field": "value",
                    "items": ["one", "two"]
        }
        """)), "orders");

        var json = JsonSerializer.SerializeToNode(actual);
        Assert.Equal("value", json?["field"]?.GetValue<string>());
                Assert.Null(json?["missing"]);
                Assert.Equal("one", json?["items"]?[0]?.GetValue<string>());
                Assert.Equal("two", json?["items"]?[1]?.GetValue<string>());
    }

    [Fact]
    public void Enrich_WhenNoConfiguredAttributes_ReturnsEmptyObject()
    {
        _configurationProvider.GetLogAttributes<string[]>("orders").Returns(System.Array.Empty<string>());
        var enricher = new DefaultLogRecord(_configurationProvider);

        var actual = enricher.Enrich(CreateRecord(JsonNode.Parse("{\"field\":\"value\"}")), "orders");

        var json = JsonSerializer.SerializeToNode(actual);
        Assert.Equal(0, json?.AsObject().Count);
    }

    private static SinkRecord CreateRecord(JsonNode value)
    {
        return new SinkRecord(new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<byte[], byte[]>()
        })
        {
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = JsonValue.Create("record-key"),
                Value = value
            }
        };
    }
}
