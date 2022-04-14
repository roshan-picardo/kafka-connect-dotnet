using System;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace Kafka.Connect.Controllers
{
    [ApiController]
    [Route("connectors")]
    public class ConnectorController : ControllerBase
    {
        private readonly ILogger<ConnectorController> _logger;
        private readonly IExecutionContext _executionContext;
        private readonly Worker _worker;

        public ConnectorController(ILogger<ConnectorController> logger, IServiceProvider serviceProvider, IExecutionContext executionContext)
        {
            _logger = logger;
            _executionContext = executionContext;
            _worker = serviceProvider.GetService<Worker>();
        }

        [HttpGet("{name}/status")]
        public IActionResult GetStatus(string name)
        {
            //var connector = _worker.Context.Connectors.SingleOrDefault(c => c.Name == name);
            //if (connector?.Connector != null) return Ok(new { status = connector.Connector.StatusLog});

            _logger.LogDebug($"Connector {name} is not active at the moment.");
            return NotFound();
        }

        [HttpPost("{name}/pause")] // Change it to put
        public async Task<IActionResult> Pause(string name)
        {
            var connector = _worker.GetConnector(name);
            if (connector == null)
            {
                _logger.LogDebug($"Connector {name} is not active at the moment.");
                return NotFound();
            }

            await connector.Pause();
            _logger.LogTrace($"Connector {name} will be paused.");

            return Ok(new {pausing = _executionContext.GetStatus(name)});
        }

        [HttpPost("{name}/resume")] // change it to put with some payload
        public async Task<IActionResult> Resume(string name, ApiPayload input)
        {
            var connector = _worker.GetConnector(name);
            if (connector == null)
            {
                _logger.LogDebug($"Connector {name} is not active at the moment.");
                return NotFound();
            }

            await connector.Resume(input?.Payload);
            _logger.LogTrace($"Connector {name} will be resumed.");

            return Ok(new {resuming = _executionContext.GetStatus(name)});
        }

        [HttpPost("{name}/restart")] // change it to put with some payload
        public async Task<IActionResult> Restart(string name, ApiPayload input)
        {
            var connector = _worker.GetConnector(name);
            if (connector == null)
            {
                _logger.LogDebug($"Connector {name} is not active at the moment.");
                return NotFound();
            }

            await connector.Restart(input?.Delay, input?.Payload);
            _logger.LogTrace($"Connector {name} will be restarted.");

            return Ok(new {restarting = _executionContext.GetStatus(name)});
        }
    }
}