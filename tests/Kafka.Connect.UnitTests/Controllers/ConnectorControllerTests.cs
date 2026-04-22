using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Controllers;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Logging;
using Microsoft.AspNetCore.Mvc;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Controllers;

public class ConnectorControllerTests
{
    private readonly ILogger<ConnectorController> _logger;
    private readonly IExecutionContext _executionContext;
    private readonly Channel<(string Connector, JsonObject Settings)> _configurationChannel;
    private readonly ConnectorController _controller;

    public ConnectorControllerTests()
    {
        _logger = Substitute.For<ILogger<ConnectorController>>();
        _executionContext = Substitute.For<IExecutionContext>();
        _configurationChannel = Channel.CreateUnbounded<(string, JsonObject)>();

        _executionContext.ConfigurationChannel.Returns(_configurationChannel);
        _executionContext.Restart(0, Arg.Any<string>(), 0).Returns(Task.CompletedTask);
        _executionContext.GetStatus(Arg.Any<string>(), 0).Returns(new { state = "connector-status" });

        _controller = new ConnectorController(_logger, _executionContext);
    }

    [Fact]
    public void GetStatus_WhenConnectorNotActive_ReturnsNotFound()
    {
        var result = _controller.GetStatus("orders");

        Assert.IsType<NotFoundResult>(result);
        _logger.Received(1).Debug("Connector orders is not active at the moment.");
    }

    [Fact]
    public void Pause_CallsExecutionContextAndReturnsStatus()
    {
        var result = _controller.Pause("orders");

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);

        _executionContext.Received(1).Pause("orders");
        _logger.Received(1).Trace("Connector orders will be paused.");
        Assert.NotNull(body["pausing"]);
    }

    [Fact]
    public void Resume_CallsExecutionContextAndReturnsStatus()
    {
        var result = _controller.Resume("orders", new ApiPayload());

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);

        _executionContext.Received(1).Resume("orders");
        _logger.Received(1).Trace("Connector orders will be resumed.");
        Assert.NotNull(body["resuming"]);
    }

    [Fact]
    public async Task Restart_CallsExecutionContextAndReturnsStatus()
    {
        var result = await _controller.Restart("orders", new ApiPayload());

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);

        await _executionContext.Received(1).Restart(0, "orders", 0);
        _logger.Received(1).Trace("Connector orders will be restarted.");
        Assert.NotNull(body["restarting"]);
    }

    [Fact]
    public async Task ListConnectors_ReturnsEmptyList()
    {
        var result = await _controller.ListConnectors();

        var ok = Assert.IsType<OkObjectResult>(result);
        var list = Assert.IsType<System.Collections.Generic.List<string>>(ok.Value);
        Assert.Empty(list);
    }

    [Fact]
    public async Task GetConnector_ReturnsOkWithEmptyConfigString()
    {
        var result = await _controller.GetConnector("orders");

        var ok = Assert.IsType<OkObjectResult>(result);
        Assert.Equal(string.Empty, ok.Value);
    }

    [Fact]
    public async Task UpdateConnector_WhenRequestIsNull_ReturnsBadRequest()
    {
        var result = await _controller.UpdateConnector("orders", null);

        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var body = ToJsonObject(badRequest.Value);
        Assert.Equal("Request body is required", body["message"]?.GetValue<string>());
    }

    [Fact]
    public async Task UpdateConnector_WhenRequestIsNotObject_ReturnsBadRequest()
    {
        var result = await _controller.UpdateConnector("orders", JsonValue.Create("text"));

        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var body = ToJsonObject(badRequest.Value);
        Assert.Equal("Request must be a JSON object", body["message"]?.GetValue<string>());
    }

    [Fact]
    public async Task UpdateConnector_WhenConnectorPropertyMissing_ReturnsBadRequest()
    {
        var request = new JsonObject
        {
            ["workers"] = new JsonArray("worker-a")
        };

        var result = await _controller.UpdateConnector("orders", request);

        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var body = ToJsonObject(badRequest.Value);
        Assert.Equal("Request must include a 'connector' property with the connector configuration", body["message"]?.GetValue<string>());
    }

    [Fact]
    public async Task UpdateConnector_WhenWorkersPropertyMissing_ReturnsBadRequest()
    {
        var request = new JsonObject
        {
            ["connector"] = new JsonObject()
        };

        var result = await _controller.UpdateConnector("orders", request);

        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        var body = ToJsonObject(badRequest.Value);
        Assert.Equal("Request must include a 'workers' property with an array of worker names", body["message"]?.GetValue<string>());
    }

    [Fact]
    public async Task UpdateConnector_WhenRequestValid_WritesToChannelAndReturnsOk()
    {
        var request = new JsonObject
        {
            ["connector"] = new JsonObject { ["tasks"] = 2 },
            ["workers"] = new JsonArray("worker-a", "worker-b")
        };

        var result = await _controller.UpdateConnector("orders", request);

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);
        Assert.Equal("Connector 'orders' updated successfully", body["message"]?.GetValue<string>());

        Assert.True(_configurationChannel.Reader.TryRead(out var payload));
        Assert.Equal("orders", payload.Connector);
        Assert.Same(request, payload.Settings);
    }

    [Fact]
    public async Task DeleteConnector_WritesNullSettingsToChannelAndReturnsOk()
    {
        var result = await _controller.DeleteConnector("orders");

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);
        Assert.Equal("Connector 'orders' deleted successfully", body["message"]?.GetValue<string>());

        Assert.True(_configurationChannel.Reader.TryRead(out var payload));
        Assert.Equal("orders", payload.Connector);
        Assert.Null(payload.Settings);
    }

    private static JsonObject ToJsonObject(object value)
    {
        return JsonSerializer.SerializeToNode(value)?.AsObject() ?? new JsonObject();
    }
}
