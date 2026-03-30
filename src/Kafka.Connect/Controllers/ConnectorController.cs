using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Logging;
using Microsoft.AspNetCore.Mvc;

namespace Kafka.Connect.Controllers;

[ApiController]
[Route("connectors")]
public class ConnectorController(
    ILogger<ConnectorController> logger,
    IExecutionContext executionContext)
    : ControllerBase
{
    [HttpGet("{name}/status")]
    public IActionResult GetStatus(string name)
    {
        //var connector = _worker.Context.Connectors.SingleOrDefault(c => c.Name == name);
        //if (connector?.Connector != null) return Ok(new { status = connector.Connector.StatusLog});

        logger.Debug($"Connector {name} is not active at the moment.");
        return NotFound();
    }

    [HttpPost("{name}/pause")] // Change it to put
    public IActionResult Pause(string name)
    {
        executionContext.Pause(name);
        logger.Trace($"Connector {name} will be paused.");

        return Ok(new {pausing = executionContext.GetStatus(name)});
    }

    [HttpPost("{name}/resume")] // change it to put with some payload
    public IActionResult Resume(string name, ApiPayload input)
    {
        executionContext.Resume(name);
        logger.Trace($"Connector {name} will be resumed.");

        return Ok(new {resuming = executionContext.GetStatus(name)});
    }

    [HttpPost("{name}/restart")] // change it to put with some payload
    public async Task<IActionResult> Restart(string name, ApiPayload input)
    {
        await executionContext.Restart(0, name);
        logger.Trace($"Connector {name} will be restarted.");

        return Ok(new {restarting = executionContext.GetStatus(name)});
    }

    [HttpGet]
    public async Task<IActionResult> ListConnectors()
    {
        var connectors = new List<string>();
        return Ok(connectors);
    }

    [HttpGet("{name}")]
    public async Task<IActionResult> GetConnector(string name)
    {
        var config = "";
        if (config == null)
        {
            return NotFound(new { message = $"Connector '{name}' not found" });
        }

        return Ok(config);
    }

    [HttpPost("{name}")]
    public async Task<IActionResult> UpdateConnector(string name, [FromBody] JsonNode request)
    {
        if (request == null)
        {
            return BadRequest(new { message = "Request body is required" });
        }

        if (request is not JsonObject requestObject)
        {
            return BadRequest(new { message = "Request must be a JSON object" });
        }

        if (!requestObject.ContainsKey("connector"))
        {
            return BadRequest(new { message = "Request must include a 'connector' property with the connector configuration" });
        }

        if (!requestObject.ContainsKey("workers"))
        {
            return BadRequest(new { message = "Request must include a 'workers' property with an array of worker names" });
        }

        await executionContext.ConfigurationChannel.Writer.WriteAsync((name, requestObject));
        
        return Ok(new { message = $"Connector '{name}' updated successfully" });
    }

    [HttpDelete("{name}")]
    public async Task<IActionResult> DeleteConnector(string name)
    {
        await executionContext.ConfigurationChannel.Writer.WriteAsync((name, null));
        return Ok(new { message = $"Connector '{name}' deleted successfully" });
    }
}