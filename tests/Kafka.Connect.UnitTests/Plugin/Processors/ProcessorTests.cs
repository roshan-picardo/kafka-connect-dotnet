using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Plugin.Processors;

public class ProcessorTests
{
    [Fact]
    public async Task Apply_WhenCalled_LoadsSettingsUsingConnectorAndProcessorName()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider
            .GetProcessorSettings<TestSettings>("orders", typeof(TestProcessor).FullName!)
            .Returns(new TestSettings { Skip = true });

        var processor = new TestProcessor(configurationProvider);
        var message = new ConnectMessage<IDictionary<string, object>>
        {
            Value = new Dictionary<string, object> { ["id"] = 1 }
        };

        var result = await processor.Apply("orders", message);

        Assert.True(result.Skip);
        Assert.Same(message, result.Flattened);
        configurationProvider.Received(1)
            .GetProcessorSettings<TestSettings>("orders", typeof(TestProcessor).FullName!);
    }

    private sealed class TestProcessor(IConfigurationProvider configurationProvider)
        : Processor<TestSettings>(configurationProvider)
    {
        protected override Task<(bool Skip, ConnectMessage<IDictionary<string, object>> Flattened)> Apply(
            TestSettings settings,
            ConnectMessage<IDictionary<string, object>> message)
        {
            return Task.FromResult((settings.Skip, message));
        }
    }

    private sealed class TestSettings
    {
        public bool Skip { get; init; }
    }
}