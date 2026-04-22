using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Providers;

public class ConnectPluginFactoryTests
{
    private readonly global::Kafka.Connect.Providers.IConfigurationProvider _configurationProvider = Substitute.For<global::Kafka.Connect.Providers.IConfigurationProvider>();

    [Fact]
    public void GetProcessor_ReturnsMatchingProcessorByTypeName()
    {
        var processor = new TestProcessor();
        var factory = CreateFactory(processors: [processor]);

        var actual = factory.GetProcessor(typeof(TestProcessor).FullName);

        Assert.Same(processor, actual);
    }

    [Fact]
    public void GetMessageConverter_ReturnsMatchingConverterByTypeName()
    {
        var converter = new TestMessageConverter();
        var factory = CreateFactory(messageConverters: [converter]);

        var actual = factory.GetMessageConverter(typeof(TestMessageConverter).FullName);

        Assert.Same(converter, actual);
    }

    [Fact]
    public void GetStrategy_ReturnsNamedStrategyWhenConfigured()
    {
        var strategy = new NamedStrategy();
        _configurationProvider.GetPluginConfig("orders").Returns(new PluginConfig
        {
            Strategy = new StrategyConfig { Name = typeof(NamedStrategy).FullName }
        });
        var factory = CreateFactory(queryStrategies: [strategy]);

        var actual = factory.GetStrategy("orders", new CommandRecord { Connector = "orders", Name = "sync", Topic = "commands", Command = new JsonObject() });

        Assert.Same(strategy, actual);
    }

    [Fact]
    public void GetStrategy_UsesSelectorResultWhenConfigured()
    {
        var fallback = new NamedStrategy();
        var selected = new SelectedStrategy();
        _configurationProvider.GetPluginConfig("orders").Returns(new PluginConfig
        {
            Strategy = new StrategyConfig
            {
                Name = typeof(NamedStrategy).FullName,
                Selector = typeof(SelectedStrategySelector).FullName,
                Settings = new Dictionary<string, string> { ["mode"] = "selected" }
            }
        });
        var factory = CreateFactory(
            strategySelectors: [new SelectedStrategySelector(selected)],
            queryStrategies: [fallback]);

        var actual = factory.GetStrategy("orders", new ConnectRecord("topic-a", 0, 1));

        Assert.Same(selected, actual);
    }

    [Fact]
    public void GetStrategy_ThrowsWhenStrategyNotDefined()
    {
        _configurationProvider.GetPluginConfig("orders").Returns(new PluginConfig());
        var factory = CreateFactory();

        Assert.Throws<ConnectDataException>(() => factory.GetStrategy("orders", new ConnectRecord("topic-a", 0, 1)));
    }

    private ConnectPluginFactory CreateFactory(
        IEnumerable<IProcessor> processors = null,
        IEnumerable<IMessageConverter> messageConverters = null,
        IEnumerable<IStrategySelector> strategySelectors = null,
        IEnumerable<IStrategy> queryStrategies = null)
    {
        return new ConnectPluginFactory(
            processors ?? Array.Empty<IProcessor>(),
            messageConverters ?? Array.Empty<IMessageConverter>(),
            strategySelectors ?? Array.Empty<IStrategySelector>(),
            queryStrategies ?? Array.Empty<IStrategy>(),
            _configurationProvider);
    }

    private sealed class TestProcessor : IProcessor
    {
        public Task<(bool Skip, ConnectMessage<IDictionary<string, object>> Flattened)> Apply(string connector, ConnectMessage<IDictionary<string, object>> message)
            => Task.FromResult((false, message));
    }

    private sealed class TestMessageConverter : IMessageConverter
    {
        public Task<byte[]> Serialize(string topic, JsonNode data, string subject = null, IDictionary<string, byte[]> headers = null, bool isValue = true)
            => Task.FromResult(Array.Empty<byte>());

        public Task<JsonNode> Deserialize(string topic, ReadOnlyMemory<byte> data, IDictionary<string, byte[]> headers, bool isValue = true)
            => Task.FromResult<JsonNode>(new JsonObject());
    }

    private sealed class NamedStrategy : IStrategy
    {
        public Task<StrategyModel<T>> Build<T>(string connector, IConnectRecord record) => Task.FromResult(new StrategyModel<T>());
    }

    private sealed class SelectedStrategy : IStrategy
    {
        public Task<StrategyModel<T>> Build<T>(string connector, IConnectRecord record) => Task.FromResult(new StrategyModel<T>());
    }

    private sealed class SelectedStrategySelector(SelectedStrategy selected) : IStrategySelector
    {
        public IStrategy GetStrategy(ConnectRecord record, IDictionary<string, string> settings = null) => selected;
    }
}
