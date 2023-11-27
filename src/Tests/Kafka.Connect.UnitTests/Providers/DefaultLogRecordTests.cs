using System.Collections.Generic;
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
    private readonly IConfigurationProvider _configurationProvider;
    private readonly DefaultLogRecord _defaultLogRecord;

    public DefaultLogRecordTests()
    {
        _configurationProvider = Substitute.For<IConfigurationProvider>();
        _defaultLogRecord = new DefaultLogRecord(_configurationProvider);
    }

    [Fact]
    public void EnrichTests()
    {
        _configurationProvider.GetLogAttributes<string[]>("connector").Returns(new[]
            { "fieldPresent", "fieldNotPresent", "fieldBoolean", "fieldInteger", "field.secondLevel" });
        var expected = new Dictionary<string, object>()
        {
            { "fieldPresent", "Exists" },
            { "fieldNotPresent", null },
            { "fieldBoolean", true },
            { "fieldInteger", 1000 },
            { "field.secondLevel", null },
        };
        var record = new SinkRecord(new ConsumeResult<byte[], byte[]>()
        {
            Message = new Message<byte[], byte[]>()
        })
        {
            Deserialized = new ConnectMessage<JsonNode>()
            {
                Key = "none",
                Value =
                    new JsonObject()
                    {
                        { "fieldPresent", "Exists" }, { "fieldBoolean", true },
                        { "fieldInteger", 1000 }, { "field", new JsonObject { { "secondLevel", "secondLevel" } } }
                    }
            }
        };
        var actual = _defaultLogRecord.Enrich(record, "connector");
        Assert.IsType<Dictionary<string, object>>(actual);
        Assert.Equivalent(expected, actual  as Dictionary<string, object>);
    }
    
    [Fact]
    public void EnrichReturnsNullTests()
    {
        _configurationProvider.GetLogAttributes<string[]>("connector").Returns(null as string[]);
        var record = new SinkRecord(new ConsumeResult<byte[], byte[]>
        {
            Message = new Message<byte[], byte[]>()
        })
        {
            Deserialized = new ConnectMessage<JsonNode>()
            {
                Key = "none",
                Value =
                    new JsonObject
                    {
                        { "fieldPresent", "Exists" }, { "fieldBoolean", true },
                        { "fieldInteger", 1000 }, { "field", new JsonObject { { "secondLevel", "secondLevel" } } }
                    }
            }
        };
        var actual = _defaultLogRecord.Enrich(record, "connector");
        Assert.Null(actual);
    }
    
    [Fact]
    public void EnrichReturnsNullValues()
    {
        _configurationProvider.GetLogAttributes<string[]>("connector").Returns(new[]
            { "fieldPresent", "fieldNotPresent", "fieldBoolean", "fieldInteger", "field.secondLevel" });
        var record = new SinkRecord(new ConsumeResult<byte[], byte[]>()
        {
            Message = new Message<byte[], byte[]>()
        })
        {
            Deserialized = new ConnectMessage<JsonNode>()
            {
                
                Key = "none",
                Value = null
            }
        };
        var actual = _defaultLogRecord.Enrich(record, "connector");
        Assert.Null(actual);
    }
}