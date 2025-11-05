using System.Collections.Concurrent;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class TestLoggingService
{
    private static readonly ConcurrentDictionary<string, DateTime> GlobalLogDeduplication = new();
    private static readonly TimeSpan GlobalDeduplicationWindow = TimeSpan.FromMilliseconds(500);
    public static void LogMessage(string message)
    {
        if (IsGlobalDuplicate(message))
        {
            return;
        }

        var timestamp = DateTime.Now.ToString("HH:mm:ss");
        if (_originalConsoleOut != null)
            _originalConsoleOut.WriteLine($"[{timestamp}] {message}");
    }

    private static TextWriter? _originalConsoleOut = null;

    private static void SetOriginalConsoleOut(TextWriter originalOut)
    {
        _originalConsoleOut = originalOut;
    }

    public static bool IsGlobalDuplicate(string message)
    {
        var now = DateTime.UtcNow;
            
        var keysToRemove = new List<string>();
        foreach (var kvp in GlobalLogDeduplication)
        {
            if (now - kvp.Value > GlobalDeduplicationWindow)
            {
                keysToRemove.Add(kvp.Key);
            }
        }
            
        foreach (var key in keysToRemove)
        {
            GlobalLogDeduplication.TryRemove(key, out _);
        }
            
        if (GlobalLogDeduplication.ContainsKey(message))
        {
            return true;
        }
            
        GlobalLogDeduplication.TryAdd(message, now);
        return false;
    }

    public void SetupTestcontainersLogging(bool detailedLog = true, bool rawJsonLog = false)
    {
        var originalOut = Console.Out;
        var originalError = Console.Error;
            
        SetOriginalConsoleOut(originalOut);
        KafkaConnectLogStream.SetOriginalConsoleOut(originalOut);
        KafkaConnectLogStream.SetRawJsonLog(rawJsonLog);
            
        Console.SetOut(new TestContainersLogWriter(originalOut, detailedLog));
        Console.SetError(new TestContainersLogWriter(originalError, detailedLog));
    }
}

public class TestContainersLogWriter(TextWriter textWriter, bool detailedLog = true) : TextWriter
{
    private static readonly ConcurrentDictionary<string, DateTime> RecentMessages = new();
    private static readonly TimeSpan MessageDeduplicationWindow = TimeSpan.FromMilliseconds(100);
    private readonly bool _detailedLog = detailedLog;

    public override System.Text.Encoding Encoding => textWriter.Encoding;

    public override void WriteLine(string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            if (IsRecentDuplicateMessage(value))
            {
                return;
            }

            if (value.Contains("[testcontainers.org"))
            {
                var message = ExtractTestcontainersMessage(value);
                
                if (!_detailedLog && IsRegexCacheMessage(message))
                {
                    return;
                }
                
                LogDockerMessage(message);
            }
            else if (value.Contains("|rdkafka#") || value.Contains("| [thrd:") || value.Contains("|PARTCNT|") || value.StartsWith("%"))
            {
                var message = ExtractKafkaMessage(value);
                LogKafkaMessage(message);
            }
            else
            {
                LogConsoleMessage(value);
            }
        }
    }

    public override void Write(string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            if (IsRecentDuplicateMessage(value))
            {
                return;
            }

            if (value.Contains("[testcontainers.org"))
            {
                var message = ExtractTestcontainersMessage(value);
                
                if (!_detailedLog && IsRegexCacheMessage(message))
                {
                    return;
                }
                
                LogDockerMessage(message);
            }
            else if (value.Contains("|rdkafka#") || value.Contains("| [thrd:") || value.Contains("|PARTCNT|") || value.StartsWith("%"))
            {
                var message = ExtractKafkaMessage(value);
                LogKafkaMessage(message);
            }
            else
            {
                LogConsoleMessage(value);
            }
        }
    }


    private void LogDockerMessage(string message)
    {
        if (message.Contains("Stop Docker container"))
        {
            return;
        }

        var formattedMessage = $"[{DateTime.Now:HH:mm:ss}] {message}";
        if (TestLoggingService.IsGlobalDuplicate(formattedMessage))
        {
            return;
        }

        var timestamp = DateTime.Now.ToString("HH:mm:ss");
        textWriter.WriteLine($"[{timestamp}] {message}");
    }

    private void LogKafkaMessage(string message)
    {
        var formattedMessage = $"[{DateTime.Now:HH:mm:ss}] {message}";
        if (TestLoggingService.IsGlobalDuplicate(formattedMessage))
        {
            return;
        }

        var timestamp = DateTime.Now.ToString("HH:mm:ss");
        textWriter.WriteLine($"[{timestamp}] {message}");
    }

    private void LogConsoleMessage(string message)
    {
        var formattedMessage = $"[{DateTime.Now:HH:mm:ss}] {message}";
        if (TestLoggingService.IsGlobalDuplicate(formattedMessage))
        {
            return;
        }

        var timestamp = DateTime.Now.ToString("HH:mm:ss");
        textWriter.WriteLine($"[{timestamp}] {message}");
    }

    private string ExtractTestcontainersMessage(string logLine)
    {
        var startIndex = logLine.IndexOf("] ");
        if (startIndex > 0 && startIndex + 2 < logLine.Length)
        {
            return logLine.Substring(startIndex + 2);
        }
        return logLine;
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

    private static bool IsRecentDuplicateMessage(string message)
    {
        var now = DateTime.UtcNow;
            
        var keysToRemove = new List<string>();
        foreach (var kvp in RecentMessages)
        {
            if (now - kvp.Value > MessageDeduplicationWindow)
            {
                keysToRemove.Add(kvp.Key);
            }
        }
            
        foreach (var key in keysToRemove)
        {
            RecentMessages.TryRemove(key, out _);
        }
            
        if (RecentMessages.ContainsKey(message))
        {
            return true;
        }
            
        RecentMessages.TryAdd(message, now);
        return false;
    }
    
    private static bool IsRegexCacheMessage(string message)
    {
        return message.Contains("Pattern") && message.Contains("added to the regex cache");
    }
}


public class KafkaConnectLogStream : Stream
{
    private readonly MemoryStream _buffer = new();
    private static readonly ConcurrentQueue<string> BufferedLogs = new();
    private static volatile bool _infrastructureReady;
    private static TextWriter? _originalConsoleOut;
    private static readonly ConcurrentDictionary<string, DateTime> RecentLogs = new();
    private static readonly TimeSpan DeduplicationWindow = TimeSpan.FromSeconds(1);
    private static bool _rawJsonLog = false;

    public static void SetOriginalConsoleOut(TextWriter originalOut)
    {
        _originalConsoleOut = originalOut;
    }

    public static void SetRawJsonLog(bool rawJsonLog)
    {
        _rawJsonLog = rawJsonLog;
    }

    public static void SetInfrastructureReady()
    {
        _infrastructureReady = true;
            
        while (BufferedLogs.TryDequeue(out var bufferedLog))
        {
            if (_originalConsoleOut != null)
            {
                _originalConsoleOut.WriteLine(bufferedLog);
            }
            else
            {
                _originalConsoleOut?.WriteLine(bufferedLog);
            }
        }
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _buffer.Length;
    public override long Position { get => _buffer.Position; set => _buffer.Position = value; }

    public override void Flush()
    {
        var content = System.Text.Encoding.UTF8.GetString(_buffer.ToArray());
        if (!string.IsNullOrWhiteSpace(content))
        {
            var lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries);
            foreach (var line in lines)
            {
                var trimmedLine = line.Trim();
                if (!string.IsNullOrEmpty(trimmedLine))
                {
                    if (IsDuplicateLog(trimmedLine))
                    {
                        continue;
                    }

                    if (!_infrastructureReady)
                    {
                        var formattedLog = FormatLogMessage(trimmedLine);
                        BufferedLogs.Enqueue(formattedLog);
                    }
                    else
                    {
                        LogKafkaConnectMessage(trimmedLine);
                    }
                }
            }
        }
        _buffer.SetLength(0);
        _buffer.Position = 0;
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => _buffer.SetLength(value);

    public override void Write(byte[] buffer, int offset, int count)
    {
        _buffer.Write(buffer, offset, count);
            
        var content = System.Text.Encoding.UTF8.GetString(_buffer.ToArray());
        if (content.Contains('\n'))
        {
            Flush();
        }
    }

    private static string FormatLogMessage(string trimmedLine)
    {
        try
        {
            var jsonDoc = System.Text.Json.JsonDocument.Parse(trimmedLine);
            if (jsonDoc.RootElement.TryGetProperty("Properties", out var props) &&
                props.TryGetProperty("Log", out var log) &&
                log.TryGetProperty("Message", out var message))
            {
                var messageText = message.GetString();
                var timestamp = DateTime.Now.ToString("HH:mm:ss");
                return $"[{timestamp}] {messageText}";
            }
            else
            {
                return trimmedLine;
            }
        }
        catch
        {
            var timestamp = DateTime.Now.ToString("HH:mm:ss");
            return $"[{timestamp}] {trimmedLine}";
        }
    }

    private static void LogKafkaConnectMessage(string trimmedLine)
    {
        if (_rawJsonLog)
        {
            // Display raw JSON as is, no additional timestamp
            if (_originalConsoleOut != null)
                _originalConsoleOut.WriteLine(trimmedLine);
        }
        else
        {
            // Parse JSON and display message with timestamp
            try
            {
                var jsonDoc = System.Text.Json.JsonDocument.Parse(trimmedLine);
                if (jsonDoc.RootElement.TryGetProperty("Properties", out var props) &&
                    props.TryGetProperty("Log", out var log) &&
                    log.TryGetProperty("Message", out var message))
                {
                    var messageText = message.GetString();
                    var timestamp = DateTime.Now.ToString("HH:mm:ss");
                    if (_originalConsoleOut != null)
                        _originalConsoleOut.WriteLine($"[{timestamp}] {messageText}");
                }
                else
                {
                    if (_originalConsoleOut != null)
                        _originalConsoleOut.WriteLine(trimmedLine);
                }
            }
            catch
            {
                var timestamp = DateTime.Now.ToString("HH:mm:ss");
                if (_originalConsoleOut != null)
                    _originalConsoleOut.WriteLine($"[{timestamp}] {trimmedLine}");
            }
        }
    }

    private static bool IsDuplicateLog(string logLine)
    {
        var now = DateTime.UtcNow;
            
        var keysToRemove = new List<string>();
        foreach (var kvp in RecentLogs)
        {
            if (now - kvp.Value > DeduplicationWindow)
            {
                keysToRemove.Add(kvp.Key);
            }
        }
            
        foreach (var key in keysToRemove)
        {
            RecentLogs.TryRemove(key, out _);
        }
            
        if (RecentLogs.ContainsKey(logLine))
        {
            return true;
        }
            
        RecentLogs.TryAdd(logLine, now);
        return false;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Flush();
            _buffer.Dispose();
        }
        base.Dispose(disposing);
    }
}