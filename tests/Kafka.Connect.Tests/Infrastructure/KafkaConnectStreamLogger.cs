using System.Collections.Concurrent;
using System.Text.Json;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

/// <summary>
/// Handles streaming logs from Kafka Connect containers
/// Buffers logs until infrastructure is ready, then streams in real-time
/// </summary>
public class KafkaConnectStreamLogger : Stream, IDisposable
{
    private readonly TextWriter _output;
    private readonly MemoryStream _buffer = new();
    private readonly ConcurrentQueue<string> _bufferedLogs = new();
    private readonly ConcurrentDictionary<string, DateTime> _recentLogs = new();
    private readonly TimeSpan _deduplicationWindow = TimeSpan.FromSeconds(1);
    
    private volatile bool _streamingEnabled = false;
    private bool _rawJsonMode = false;
    private bool _disposed = false;

    public KafkaConnectStreamLogger(TextWriter output)
    {
        _output = output ?? throw new ArgumentNullException(nameof(output));
    }

    public void StartStreaming()
    {
        _streamingEnabled = true;
        FlushBufferedLogs();
    }

    public void SetRawJsonMode(bool enabled)
    {
        _rawJsonMode = enabled;
    }

    public void LogMessage(string message)
    {
        if (string.IsNullOrWhiteSpace(message) || _disposed)
            return;

        if (IsDuplicateLog(message))
            return;

        if (!_streamingEnabled)
        {
            var formattedLog = FormatLogMessage(message);
            _bufferedLogs.Enqueue(formattedLog);
        }
        else
        {
            WriteKafkaConnectMessage(message);
        }
    }

    private void FlushBufferedLogs()
    {
        while (_bufferedLogs.TryDequeue(out var bufferedLog))
        {
            _output.WriteLine(bufferedLog);
        }
    }

    private string FormatLogMessage(string logLine)
    {
        if (_rawJsonMode)
        {
            return logLine; // Return raw JSON as is
        }

        try
        {
            var jsonDoc = JsonDocument.Parse(logLine);
            if (jsonDoc.RootElement.TryGetProperty("Properties", out var props) &&
                props.TryGetProperty("Log", out var log) &&
                log.TryGetProperty("Message", out var message))
            {
                var messageText = message.GetString();
                var timestamp = DateTime.Now.ToString("HH:mm:ss");
                return $"[{timestamp}] {messageText}";
            }
            
            return logLine;
        }
        catch
        {
            var timestamp = DateTime.Now.ToString("HH:mm:ss");
            return $"[{timestamp}] {logLine}";
        }
    }

    private void WriteKafkaConnectMessage(string logLine)
    {
        if (_rawJsonMode)
        {
            _output.WriteLine(logLine);
            return;
        }

        try
        {
            var jsonDoc = JsonDocument.Parse(logLine);
            if (jsonDoc.RootElement.TryGetProperty("Properties", out var props) &&
                props.TryGetProperty("Log", out var log) &&
                log.TryGetProperty("Message", out var message))
            {
                var messageText = message.GetString();
                var timestamp = DateTime.Now.ToString("HH:mm:ss");
                _output.WriteLine($"[{timestamp}] {messageText}");
            }
            else
            {
                _output.WriteLine(logLine);
            }
        }
        catch
        {
            var timestamp = DateTime.Now.ToString("HH:mm:ss");
            _output.WriteLine($"[{timestamp}] {logLine}");
        }
    }

    private bool IsDuplicateLog(string logLine)
    {
        var now = DateTime.UtcNow;
        
        // Clean up old entries
        var keysToRemove = new List<string>();
        foreach (var kvp in _recentLogs)
        {
            if (now - kvp.Value > _deduplicationWindow)
            {
                keysToRemove.Add(kvp.Key);
            }
        }
        
        foreach (var key in keysToRemove)
        {
            _recentLogs.TryRemove(key, out _);
        }
        
        // Check for duplicate
        if (_recentLogs.ContainsKey(logLine))
        {
            return true;
        }
        
        _recentLogs.TryAdd(logLine, now);
        return false;
    }

    #region Stream Implementation

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _buffer.Length;
    public override long Position 
    { 
        get => _buffer.Position; 
        set => _buffer.Position = value; 
    }

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
                    LogMessage(trimmedLine);
                }
            }
        }
        _buffer.SetLength(0);
        _buffer.Position = 0;
    }

    public override int Read(byte[] buffer, int offset, int count) => 
        throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => 
        throw new NotSupportedException();

    public override void SetLength(long value) => 
        _buffer.SetLength(value);

    public override void Write(byte[] buffer, int offset, int count)
    {
        _buffer.Write(buffer, offset, count);
        
        var content = System.Text.Encoding.UTF8.GetString(_buffer.ToArray());
        if (content.Contains('\n'))
        {
            Flush();
        }
    }

    #endregion

    protected override void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            Flush();
            _buffer.Dispose();
            _recentLogs.Clear();
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}