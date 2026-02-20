using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Hosting;

namespace Kafka.Connect.Controllers;

[ApiController]
[Route("workers")]
public class WorkerController(IHostedService hostedService, IExecutionContext executionContext)
    : ControllerBase
{
    [HttpGet("status")]
    public IActionResult Status()
    {
        return Ok(new {status = executionContext.GetSimpleStatus()});
    }
        
    [HttpGet("version")]
    public IActionResult Version()
    {
        return Ok(new {versions = new
        {
            Runtime = Environment.Version.ToString(),
            Library = Library.VersionString,
            Connect = Assembly.GetExecutingAssembly().GetName().Version?.ToString(),
            Extends = Environment.GetEnvironmentVariable("APPLICATION_VERSION")
        }});
    }

    [HttpPost("pause")] 
    public IActionResult Pause()
    {
        executionContext.Pause();
        return Ok(new {pausing = executionContext.GetStatus()});
    }
        
    [HttpPost("resume")] 
    public IActionResult Resume()
    {
        executionContext.Resume();
        return Ok(new {resuming = executionContext.GetStatus()});
    }
        
    [HttpPost("stop")] 
    public async Task<IActionResult> Stop()
    {
        await hostedService.StopAsync(default);
        return Ok(new {stopping = executionContext.GetStatus()});
    }
        
    [HttpPost("start")] 
    public async Task<IActionResult> Start()
    {
        await hostedService.StartAsync(new CancellationToken());
        return Ok(new {starting = executionContext.GetStatus()});
    }
        
    [HttpPost("restart")] 
    public async Task<IActionResult> Restart(ApiPayload input)
    {
        await executionContext.Restart(0);
        return Ok(new {restarting = executionContext.GetStatus()});
    }
}