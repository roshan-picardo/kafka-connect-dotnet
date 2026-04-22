using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;
using IConfigurationProvider = Kafka.Connect.Providers.IConfigurationProvider;

namespace UnitTests.Kafka.Connect.Handlers;

public class MessageHandlerTests
{
    private readonly ILogger<MessageHandler> _logger = Substitute.For<ILogger<MessageHandler>>();
    private readonly IConnectPluginFactory _connectPluginFactory = Substitute.For<IConnectPluginFactory>();
    private readonly IConfigurationProvider _configurationProvider = Substitute.For<IConfigurationProvider>();
    private readonly MessageHandler _handler;

    public MessageHandlerTests()
    {
        _handler = new MessageHandler(_logger, _connectPluginFactory, _configurationProvider);
    }

    [Fact]
    public async Task Process_WhenNoProcessorConfig_ReturnsOriginalMessage()
    {
        var message = new ConnectMessage<JsonNode>
        {
            Key = JsonNode.Parse("{\"id\":1}"),
            Value = JsonNode.Parse("{\"name\":\"alice\"}")
        };
        _configurationProvider.GetMessageProcessors("connector", "topic").Returns(new List<ProcessorConfig>());

        var result = await _handler.Process("connector", "topic", message);

        Assert.False(result.Skip);
        Assert.Equal(message.Key?.ToJsonString(), result.Message.Key?.ToJsonString());
        Assert.Equal(message.Value?.ToJsonString(), result.Message.Value?.ToJsonString());
    }

    [Fact]
    public async Task Process_WhenProcessorNotRegistered_ContinuesAndLogsTrace()
    {
        var message = new ConnectMessage<JsonNode>
        {
            Key = JsonNode.Parse("{\"id\":1}"),
            Value = JsonNode.Parse("{\"name\":\"alice\"}")
        };
        _configurationProvider.GetMessageProcessors("connector", "topic")
            .Returns(new List<ProcessorConfig> { new() { Name = "proc-a" } });
        _connectPluginFactory.GetProcessor("proc-a").Returns((IProcessor)null);

        var result = await _handler.Process("connector", "topic", message);

        Assert.False(result.Skip);
        _logger.Received(1).Trace("Processor is not registered.", Arg.Any<object>());
    }

    [Fact]
    public async Task Process_WhenProcessorReturnsSkip_StopsFurtherProcessing()
    {
        var message = new ConnectMessage<JsonNode>
        {
            Key = JsonNode.Parse("{\"id\":1}"),
            Value = JsonNode.Parse("{\"name\":\"alice\"}")
        };

        var proc1 = Substitute.For<IProcessor>();
        var proc2 = Substitute.For<IProcessor>();

        _configurationProvider.GetMessageProcessors("connector", "topic")
            .Returns(new List<ProcessorConfig>
            {
                new() { Name = "proc-a" },
                new() { Name = "proc-b" }
            });
        _connectPluginFactory.GetProcessor("proc-a").Returns(proc1);
        _connectPluginFactory.GetProcessor("proc-b").Returns(proc2);

        proc1.Apply(Arg.Any<string>(), Arg.Any<ConnectMessage<IDictionary<string, object>>>())
            .Returns((true, message.Convert()));

        var result = await _handler.Process("connector", "topic", message);

        Assert.True(result.Skip);
        await proc2.DidNotReceive().Apply(Arg.Any<string>(), Arg.Any<ConnectMessage<IDictionary<string, object>>>());
    }

    [Fact]
    public async Task Serialize_UsesConfiguredConverters()
    {
        var converterConfig = new ConverterConfig
        {
            Key = "key-converter",
            Value = "value-converter",
            Subject = "Topic",
            Record = "orders"
        };
        _configurationProvider.GetMessageConverters("connector", "topic-a").Returns(converterConfig);

        var keyConverter = Substitute.For<IMessageConverter>();
        var valueConverter = Substitute.For<IMessageConverter>();
        _connectPluginFactory.GetMessageConverter("key-converter").Returns(keyConverter);
        _connectPluginFactory.GetMessageConverter("value-converter").Returns(valueConverter);

        keyConverter.Serialize(Arg.Any<string>(), Arg.Any<JsonNode>(), Arg.Any<string>(), null, true)
            .Returns(new byte[] { 1 });
        valueConverter.Serialize(Arg.Any<string>(), Arg.Any<JsonNode>(), Arg.Any<string>(), null, true)
            .Returns(new byte[] { 2 });

        var message = new ConnectMessage<JsonNode>
        {
            Key = JsonNode.Parse("{\"id\":1}"),
            Value = JsonNode.Parse("{\"name\":\"alice\"}")
        };

        var result = await _handler.Serialize("connector", "topic-a", message);

        Assert.Equal(new byte[] { 1 }, result.Key);
        Assert.Equal(new byte[] { 2 }, result.Value);
        await keyConverter.Received(1).Serialize("topic-a", message.Key, Arg.Any<string>(), null, true);
        await valueConverter.Received(1).Serialize("topic-a", message.Value, Arg.Any<string>(), null, true);
    }

    [Fact]
    public async Task Deserialize_WhenConverterReturnsNull_DefaultsToEmptyObject()
    {
        var converterConfig = new ConverterConfig
        {
            Key = "key-converter",
            Value = "value-converter",
            Subject = "Topic",
            Record = "orders"
        };
        _configurationProvider.GetMessageConverters("connector", "topic-a").Returns(converterConfig);

        var keyConverter = Substitute.For<IMessageConverter>();
        var valueConverter = Substitute.For<IMessageConverter>();
        _connectPluginFactory.GetMessageConverter("key-converter").Returns(keyConverter);
        _connectPluginFactory.GetMessageConverter("value-converter").Returns(valueConverter);

        keyConverter.Deserialize(Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<IDictionary<string, byte[]>>(), false)
            .Returns((JsonNode)null);
        valueConverter.Deserialize(Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<IDictionary<string, byte[]>>(), true)
            .Returns(JsonNode.Parse("{\"ok\":true}"));

        var result = await _handler.Deserialize(
            "connector",
            "topic-a",
            new ConnectMessage<byte[]>
            {
                Key = new byte[] { 1 },
                Value = new byte[] { 2 },
                Headers = new Dictionary<string, byte[]>()
            });

        Assert.Equal("{}", result.Key?.ToJsonString());
        Assert.Equal(true, result.Value?["ok"]?.GetValue<bool>());
        await keyConverter.Received(1).Deserialize("topic-a", Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<IDictionary<string, byte[]>>(), false);
        await valueConverter.Received(1).Deserialize("topic-a", Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<IDictionary<string, byte[]>>(), true);
    }
}