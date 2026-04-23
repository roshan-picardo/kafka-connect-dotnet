using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using Xunit;

namespace UnitTests.Kafka.Connect.Plugin.Strategies;

public class StrategyTests
{
    [Fact]
    public async Task Build_WhenConnectRecordProvided_UsesConnectRecordImplementation()
    {
        var record = new ConnectRecord("orders", 2, 10)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1, 2, 3] }
        };
        var strategy = new TestStrategy();

        var result = await strategy.Build<string>("connector-a", record);

        Assert.Equal(Status.Processed, result.Status);
        Assert.Equal("orders", result.Topic);
        Assert.Equal(2, result.Partition);
        Assert.Equal(10, result.Offset);
        Assert.Equal(record.Key, result.Key);
        Assert.Equal(["connector-a:connect"], result.Models);
    }

    [Fact]
    public async Task Build_WhenCommandRecordProvided_UsesCommandRecordImplementation()
    {
        var record = new CommandRecord
        {
            Connector = "connector-a",
            Name = "sync",
            Topic = "commands",
            Partition = 1,
            Offset = 4
        };
        var strategy = new TestStrategy();

        var result = await strategy.Build<string>("connector-a", record);

        Assert.Equal(Status.Selected, result.Status);
        Assert.Equal("commands", result.Topic);
        Assert.Equal(1, result.Partition);
        Assert.Equal(4, result.Offset);
        Assert.Equal(record.Key, result.Key);
        Assert.Equal(["connector-a:command"], result.Models);
    }

    [Fact]
    public async Task Build_WhenUnknownRecordProvided_ReturnsSkippingModel()
    {
        var record = new UnknownRecord
        {
            Topic = "fallback",
            Partition = 3,
            Offset = 8,
            Key = Guid.NewGuid()
        };
        var strategy = new TestStrategy();

        var result = await strategy.Build<string>("connector-a", record);

        Assert.Equal(Status.Skipping, result.Status);
        Assert.Equal("fallback", result.Topic);
        Assert.Equal(3, result.Partition);
        Assert.Equal(8, result.Offset);
        Assert.Null(result.Models);
    }

    [Fact]
    public void BuildCondition_ReplacesParametersFromFlattenedDictionary()
    {
        var strategy = new TestStrategy();
        var flattened = new Dictionary<string, object>
        {
            ["customer.name"] = "Ada",
            ["customer.id"] = 7
        };

        var result = strategy.ExposedBuildCondition("name = #customer.name# and id = #customer.id#", flattened);

        Assert.Equal("name = Ada and id = 7", result);
    }

    [Fact]
    public void GetConditionParameters_ReturnsMatchedParametersInOrder()
    {
        var strategy = new TestStrategy();

        var result = strategy.ExposedGetConditionParameters("a = #first# and b = #second#");

        Assert.Equal(["first", "second"], result);
    }

    [Fact]
    public void GetValueByType_FormatsNullStringsAndValues()
    {
        var strategy = new TestStrategy();

        Assert.Equal("NULL", strategy.ExposedGetValueByType(null));
        Assert.Equal("'Ada''s'", strategy.ExposedGetValueByType("Ada's"));
        Assert.Equal("'5'", strategy.ExposedGetValueByType(5));
    }

    private sealed class TestStrategy : Strategy<string>
    {
        public string ExposedBuildCondition(string condition, IDictionary<string, object> flattened) =>
            BuildCondition(condition, flattened);

        public List<string> ExposedGetConditionParameters(string condition) =>
            GetConditionParameters(condition);

        public string ExposedGetValueByType(object value) => GetValueByType(value);

        protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
        {
            return Task.FromResult(new StrategyModel<string>
            {
                Status = Status.Processed,
                Models = [$"{connector}:connect"]
            });
        }

        protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        {
            return Task.FromResult(new StrategyModel<string>
            {
                Status = Status.Selected,
                Models = [$"{connector}:command"]
            });
        }
    }

    private sealed class UnknownRecord : IConnectRecord
    {
        public Exception Exception { get; set; }
        public string Topic { get; init; }
        public int Partition { get; init; }
        public long Offset { get; init; }
        public Guid Key { get; init; }
        public Status Status { get; set; }
    }
}