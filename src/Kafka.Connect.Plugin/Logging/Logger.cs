using System;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Plugin.Logging;

public class Logger<T> : ILogger<T> where T : class
{
    private readonly Microsoft.Extensions.Logging.ILogger<T> _logger;
    private readonly Microsoft.Extensions.Logging.ILogger<SinkLog> _sinkLogger;

    public Logger(
        Microsoft.Extensions.Logging.ILogger<T> logger,
        Microsoft.Extensions.Logging.ILogger<SinkLog> sinkLogger)
    {
        _logger = logger;
        _sinkLogger = sinkLogger;
    }

    private void Log(LogLevel level, string message, Exception exception = null, object data = null)
    {
        if (data is Exception exp)
        {
            data = null;
        }
        else
        {
            exp = exception;
        }

        switch (exp)
        {
            case null when data == null:
                _logger.Log(level, "{@Log}", new { Message = message });
                break;
            case null:
                _logger.Log(level, "{@Log}", new { Message = message, Data = data });
                break;
            default:
            {
                if (data == null)
                {
                    _logger.Log(level, exp, "{@Log}", new { Message = message });
                }
                else
                {
                    _logger.Log(level, exp, "{@Log}", new { Message = message, Data = data });
                }

                break;
            }
        }
    }

    public void Trace(string message, object data = null, Exception exception = null) =>
        Log(LogLevel.Trace, message, exception, data);

    public void Debug(string message, object data = null, Exception exception = null) =>
        Log(LogLevel.Debug, message, exception, data);

    public void Info(string message, object data = null, Exception exception = null) =>
        Log(LogLevel.Information, message, exception, data);

    public void Warning(string message, object data = null, Exception exception = null) =>
        Log(LogLevel.Warning, message, exception, data);

    public void Error(string message, object data = null, Exception exception = null) =>
        Log(LogLevel.Error, message, exception, data);

    public void Critical(string message, object data = null, Exception exception = null) =>
        Log(LogLevel.Critical, message, exception, data);

    public void None(string message, object data = null, Exception exception = null) =>
        Log(LogLevel.None, message, exception, data);

    public void Record(object data, Exception exception) =>
        _logger.Log(LogLevel.Information, exception, "{@Record}", data);

    public void Document(ConnectMessage<JsonNode> document) => _sinkLogger.Log(LogLevel.Debug, "{@Document}",
        new { Key = document.Key?.ToNestedDictionary(), Value = document.Value?.ToNestedDictionary() });

    public void Health(object health) => _sinkLogger.Log(LogLevel.Information, "{@Health}", health);

    public SinkLog Track(string message) => new(_logger, message);
}