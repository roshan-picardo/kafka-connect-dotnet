using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Logging;
using Microsoft.AspNetCore.Mvc;

namespace Kafka.Connect.Controllers
{
    [ApiController]
    [Route("connectors")]
    public class ConnectorController : ControllerBase
    {
        private readonly ILogger<ConnectorController> _logger;
        private readonly IExecutionContext _executionContext;

        public ConnectorController(ILogger<ConnectorController> logger, IExecutionContext executionContext)
        {
            _logger = logger;
            _executionContext = executionContext;
        }

        [HttpGet("{name}/status")]
        public IActionResult GetStatus(string name)
        {
            //var connector = _worker.Context.Connectors.SingleOrDefault(c => c.Name == name);
            //if (connector?.Connector != null) return Ok(new { status = connector.Connector.StatusLog});

            _logger.Debug($"Connector {name} is not active at the moment.");
            return NotFound();
        }

        [HttpPost("{name}/pause")] // Change it to put
        public async Task<IActionResult> Pause(string name)
        {
            await _executionContext.Pause(name);
            _logger.Trace($"Connector {name} will be paused.");

            return Ok(new {pausing = _executionContext.GetStatus(name)});
        }

        [HttpPost("{name}/resume")] // change it to put with some payload
        public async Task<IActionResult> Resume(string name, ApiPayload input)
        {
            await _executionContext.Resume(name);
            _logger.Trace($"Connector {name} will be resumed.");

            return Ok(new {resuming = _executionContext.GetStatus(name)});
        }

        [HttpPost("{name}/restart")] // change it to put with some payload
        public async Task<IActionResult> Restart(string name, ApiPayload input)
        {
            await _executionContext.Restart(0, name);
            _logger.Trace($"Connector {name} will be restarted.");

            return Ok(new {restarting = _executionContext.GetStatus(name)});
        }
    }
}