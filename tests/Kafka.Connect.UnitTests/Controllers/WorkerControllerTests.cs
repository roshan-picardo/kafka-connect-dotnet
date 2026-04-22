using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Controllers;
using Kafka.Connect.Models;
using Kafka.Connect.Connectors;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Controllers;

public class WorkerControllerTests
{
    private readonly IHostedService _hostedService;
    private readonly IExecutionContext _executionContext;
    private readonly WorkerController _controller;

    public WorkerControllerTests()
    {
        _hostedService = Substitute.For<IHostedService>();
        _executionContext = Substitute.For<IExecutionContext>();

        _hostedService.StartAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        _hostedService.StopAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        _executionContext.Restart(0).Returns(Task.CompletedTask);
        _executionContext.GetStatus().Returns(new { state = "running" });
        _executionContext.GetSimpleStatus().Returns(new { state = "simple" });

        _controller = new WorkerController(_hostedService, _executionContext);
    }

    [Fact]
    public void Status_ReturnsOkWithStatusPayload()
    {
        var result = _controller.Status();

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);
        Assert.NotNull(body["status"]);
    }

    [Fact]
    public void Version_ReturnsOkWithVersionFields()
    {
        var result = _controller.Version();

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);
        var versions = body["versions"]?.AsObject();

        Assert.NotNull(versions);
        Assert.NotNull(versions["Runtime"]);
        Assert.NotNull(versions["Library"]);
        Assert.NotNull(versions["Connect"]);
    }

    [Fact]
    public void Pause_CallsExecutionContextPauseAndReturnsStatus()
    {
        var result = _controller.Pause();

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);

        _executionContext.Received(1).Pause();
        Assert.NotNull(body["pausing"]);
    }

    [Fact]
    public void Resume_CallsExecutionContextResumeAndReturnsStatus()
    {
        var result = _controller.Resume();

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);

        _executionContext.Received(1).Resume();
        Assert.NotNull(body["resuming"]);
    }

    [Fact]
    public async Task Stop_CallsHostedServiceStopAndReturnsStatus()
    {
        var result = await _controller.Stop();

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);

        await _hostedService.Received(1).StopAsync(default);
        Assert.NotNull(body["stopping"]);
    }

    [Fact]
    public async Task Start_CallsHostedServiceStartAndReturnsStatus()
    {
        var result = await _controller.Start();

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);

        await _hostedService.Received(1).StartAsync(Arg.Any<CancellationToken>());
        Assert.NotNull(body["starting"]);
    }

    [Fact]
    public async Task Restart_CallsExecutionContextRestartWithZeroDelay()
    {
        var result = await _controller.Restart(new ApiPayload());

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = ToJsonObject(ok.Value);

        await _executionContext.Received(1).Restart(0);
        Assert.NotNull(body["restarting"]);
    }

    private static JsonObject ToJsonObject(object value)
    {
        return JsonSerializer.SerializeToNode(value)?.AsObject() ?? new JsonObject();
    }
}
