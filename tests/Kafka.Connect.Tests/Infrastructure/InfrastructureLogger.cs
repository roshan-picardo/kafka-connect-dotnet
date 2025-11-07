using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

/// <summary>
/// Handles logging for infrastructure setup and cleanup phases
/// Manages TestContainers and custom logs with deduplication
/// </summary>
public class InfrastructureLogger : IDisposable
{
    private readonly TextWriter _output;
    private readonly ConcurrentDictionary<string, DateTime> _recentMessages = new();
    private readonly TimeSpan _deduplicationWindow = TimeSpan.FromMilliseconds(500);
    private static readonly Regex TestContainersPattern = new(@"\[testcontainers\.org", RegexOptions.Compiled);
    private static readonly Regex KafkaClientPattern = new(@"\|rdkafka#|\| \[thrd:|\|PARTCNT\||^%", RegexOptions.Compiled);
    private bool _disposed = false;

    public InfrastructureLogger(TextWriter output)
    {
        _output = output ?? throw new ArgumentNullException(nameof(output));
    }

    public void LogMessage(string message, LogSource source = LogSource.Infrastructure)
    {
        if (string.IsNullOrWhiteSpace(message) || _disposed)
            return;

        if (IsDuplicateMessage(message))
            return;

        var formattedMessage = FormatMessage(message, source);
        if (!string.IsNullOrEmpty(formattedMessage))
        {
            _output.WriteLine(formattedMessage);
        }
    }

    private string FormatMessage(string message, LogSource source)
    {
        var timestamp = DateTime.Now.ToString("HH:mm:ss");
        
        return source switch
        {
            LogSource.TestContainers => FormatTestContainersMessage(message, timestamp),
            LogSource.Infrastructure => $"[{timestamp}] {message}",
            LogSource.Custom => $"[{timestamp}] {message}",
            _ => $"[{timestamp}] {message}"
        };
    }

    private string FormatTestContainersMessage(string message, string timestamp)
    {
        // Extract meaningful part from TestContainers log
        if (TestContainersPattern.IsMatch(message))
        {
            var startIndex = message.IndexOf("] ");
            if (startIndex > 0 && startIndex + 2 < message.Length)
            {
                var extractedMessage = message.Substring(startIndex + 2);
                
                // Skip certain verbose messages
                if (IsVerboseTestContainersMessage(extractedMessage))
                    return string.Empty;
                    
                return $"[{timestamp}] {extractedMessage}";
            }
        }
        
        // Handle Kafka client messages
        if (KafkaClientPattern.IsMatch(message))
        {
            var kafkaMessage = ExtractKafkaMessage(message);
            return $"[{timestamp}] {kafkaMessage}";
        }
        
        return $"[{timestamp}] {message}";
    }

    private string ExtractKafkaMessage(string logLine)
    {
        var parts = logLine.Split('|');
        if (parts.Length >= 5)
        {
            var component = parts[3];
            var messageStart = logLine.IndexOf("]: ");
            if (messageStart > 0 && messageStart + 3 < logLine.Length)
            {
                var message = logLine.Substring(messageStart + 3);
                return $"{component}: {message}";
            }
        }
        return logLine;
    }

    private bool IsVerboseTestContainersMessage(string message)
    {
        return message.Contains("Pattern") && message.Contains("added to the regex cache") ||
               message.Contains("Stop Docker container");
    }

    private bool IsDuplicateMessage(string message)
    {
        var now = DateTime.UtcNow;
        
        // Clean up old entries
        var keysToRemove = new List<string>();
        foreach (var kvp in _recentMessages)
        {
            if (now - kvp.Value > _deduplicationWindow)
            {
                keysToRemove.Add(kvp.Key);
            }
        }
        
        foreach (var key in keysToRemove)
        {
            _recentMessages.TryRemove(key, out _);
        }
        
        // Check for duplicate
        if (_recentMessages.ContainsKey(message))
        {
            return true;
        }
        
        _recentMessages.TryAdd(message, now);
        return false;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _recentMessages.Clear();
        }
    }
}