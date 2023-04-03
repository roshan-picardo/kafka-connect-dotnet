using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;


namespace Kafka.Connect.Controllers
{
    [ApiController]
    [Route("workers")]
    public class WorkerController : ControllerBase
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IHostedService _hostedService;
        private readonly IExecutionContext _executionContext;

        public WorkerController(IServiceProvider serviceProvider, IHostedService hostedService, IExecutionContext executionContext)
        {
            _serviceProvider = serviceProvider;
            _hostedService = hostedService;
            _executionContext = executionContext;
        }

        [HttpGet("status")]
        public IActionResult Status()
        {
            return Ok(new {status = _executionContext.GetStatus()});
        }
        
        [HttpGet("version")]
        public IActionResult Version()
        {
            return Ok(new {version = new
            {
                Dotnet = Environment.Version.ToString(),
                Library = Library.VersionString,
                Connect = Assembly.GetExecutingAssembly().GetName().Version?.ToString(),
                Application = Environment.GetEnvironmentVariable("APPLICATION_VERSION")
            }});
        }

        [HttpPost("pause")] 
        public async Task<IActionResult> Pause()
        {
            var worker = _serviceProvider.GetService<Worker>();
            await worker.PauseAsync();
            return Ok(new {pausing = _executionContext.GetStatus()});
        }
        
        [HttpPost("resume")] 
        public async Task<IActionResult> Resume()
        {
            var worker = _serviceProvider.GetService<Worker>();
            await _serviceProvider.GetService<Worker>().ResumeAsync();
            return Ok(new {resuming = _executionContext.GetStatus()});
        }
        
        [HttpPost("stop")] 
        public async Task<IActionResult> Stop()
        {
            await _hostedService.StopAsync(default);
            return Ok(new {stopping = _executionContext.GetStatus()});
        }
        
        [HttpPost("start")] 
        public async Task<IActionResult> Start()
        {
            await _hostedService.StartAsync(new CancellationToken());
            return Ok(new {starting = _executionContext.GetStatus()});
        }
        
        [HttpPost("restart")] 
        public async Task<IActionResult> Restart(ApiPayload input)
        {
            await _serviceProvider.GetService<Worker>().RestartAsync(input?.Delay);
            return Ok(new {restarting = _executionContext.GetStatus()});
        }
    }
}