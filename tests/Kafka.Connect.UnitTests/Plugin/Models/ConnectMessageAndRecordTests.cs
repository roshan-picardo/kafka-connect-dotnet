using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.Plugin.Models;

public class ConnectMessageAndRecordTests
{
    [Fact]
    public void ConnectMessageConvert_WhenJsonNodeMessageProvided_ConvertsToFlattenedDictionary()
    {
        var message = new ConnectMessage<JsonNode>
        {
            Key = JsonNode.Parse("""{ "id": 5 }"""),
            Value = JsonNode.Parse("""{ "customer": { "name": "Ada" } }""")
        };

        var result = message.Convert();

        Assert.Equal(5, result.Key!["id"]);
        Assert.Equal("Ada", result.Value!["customer.name"]);
    }

    [Fact]
    public void ConnectMessageGetValue_WhenMultipleKeysProvided_ReturnsFirstMatch()
    {
        var message = new ConnectMessage<JsonNode>
        {
            Value = JsonNode.Parse("""{ "alternate": "Ada" }""")
        };

        var result = message.GetValue<string>("missing", "alternate");

        Assert.Equal("Ada", result);
    }

    [Fact]
    public void ConnectMessageConvertGeneric_WhenTypedTargetProvided_DeserializesValues()
    {
        var message = new ConnectMessage<JsonNode>
        {
            Key = JsonValue.Create(10),
            Value = JsonValue.Create("Ada")
        };

        var result = message.Convert<int, string>();

        Assert.Equal(10, result.Key);
        Assert.Equal("Ada", result.Value);
    }

    [Fact]
    public void ConnectMessageConvertGeneric_WhenNullsProvided_UsesEmptyObjects()
    {
        var message = new ConnectMessage<int?, ValueModel>
        {
            Key = null,
            Value = null
        };

        var result = message.Convert();

        Assert.Equal("{}", result.Key!.ToJsonString());
        Assert.Equal("{}", result.Value!.ToJsonString());
    }

    [Fact]
    public void ConnectRecord_WhenConstructed_SetsCoordinatesAndComputesKey()
    {
        var record = new ConnectRecord("orders", 2, 14)
        {
            Serialized = new ConnectMessage<byte[]>
            {
                Key = JsonSerializer.SerializeToUtf8Bytes(new { id = 99 })
            }
        };

        Assert.Equal("orders", record.Topic);
        Assert.Equal(2, record.Partition);
        Assert.Equal(14, record.Offset);
        Assert.NotEqual(Guid.Empty, record.Key);
        Assert.True(record.LogTimestamp.Created > 0);
        Assert.True(record.LogTimestamp.Consumed > 0);
    }

    [Fact]
    public void Published_WhenStatusIsPublishing_UpdatesCoordinatesAndStatus()
    {
        var record = new ConnectRecord("source", 0, 1)
        {
            Status = Status.Publishing
        };

        record.Published("target", 4, 22);

        Assert.Equal("target", record.Topic);
        Assert.Equal(4, record.Partition);
        Assert.Equal(22, record.Offset);
        Assert.Equal(Status.Published, record.Status);
    }

    [Fact]
    public void UpdateStatus_WhenFailedRequested_TransitionsToFailed()
    {
        var record = new ConnectRecord("orders", 0, 1)
        {
            Status = Status.Updating
        };

        record.UpdateStatus(failed: true);

        Assert.Equal(Status.Failed, record.Status);
    }

    [Fact]
    public void EndTiming_WhenBatchCompleted_ReturnsExpectedBatchMetadata()
    {
        var record = new ConnectRecord("orders", 0, 1);
        record.LogTimestamp.Created = 1_000;
        record.LogTimestamp.Consumed = 1_500;

        var result = record.EndTiming(batchSize: 4, millis: 2_500);
        var json = JsonNode.Parse(JsonSerializer.Serialize(result))!;

        Assert.Equal(250m, json["Duration"]!.GetValue<decimal>());
        Assert.Equal(4, json["Batch"]!["Size"]!.GetValue<int>());
        Assert.Equal(1000L, json["Batch"]!["Total"]!.GetValue<long>());
    }

    [Fact]
    public void SetModel_WhenReferenceTypeStored_GetModelReturnsSameInstance()
    {
        var record = new ConnectRecord("orders", 0, 1);
        var model = new ValueModel { Name = "tracked" };

        record.SetModel(model);

        Assert.Same(model, record.GetModel<ValueModel>());
    }

    [Fact]
    public void IsOfAndStatusFlags_ReturnExpectedValues()
    {
        var record = new ConnectRecord("orders", 3, 9)
        {
            Status = Status.Processed
        };

        Assert.True(record.IsOf("orders", 3, 9));
        Assert.True(record.Sinking);
        Assert.True(record.Publishing);
        Assert.False(record.Processing);
    }

    private sealed class ValueModel
    {
        public string Name { get; init; }
    }
}