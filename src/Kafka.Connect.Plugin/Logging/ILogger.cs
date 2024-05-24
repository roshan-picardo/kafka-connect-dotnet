using System;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Logging;

public interface ILogger<T> where T : class
{
    void Trace(string message, object data = null, Exception exception = null);
    void Debug(string message, object data = null, Exception exception = null);
    void Info(string message, object data = null, Exception exception = null);
    void Warning(string message, object data = null, Exception exception = null);
    void Error(string message, object data = null, Exception exception = null);
    void Critical(string message, object data = null, Exception exception = null);
    void None(string message, object data = null, Exception exception = null);
    void Record(object data, Exception exception);
    void Health(dynamic health);
    void Document(ConnectMessage<JsonNode> document);
    ConnectLog Track(string message);
}