using System.Collections.Concurrent;
using System.Text.Json;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

/// <summary>
/// Simple buffer for Kafka Connect logs that displays them at the end of test execution
/// </summary>
public class KafkaConnectLogBuffer : Stream
{
    private readonly MemoryStream _buffer = new();
    private static readonly ConcurrentQueue<string> BufferedLogs = new();
    private static readonly object Lock = new();
    private static bool _logsDisplayed = false;
    private static bool _rawJsonMode = false;
    private bool _disposed = false;
    
    static KafkaConnectLogBuffer()
    {
        // Register to display logs when the application domain is unloading
        AppDomain.CurrentDomain.ProcessExit += (sender, e) => DisplayBufferedLogs();
        AppDomain.CurrentDomain.DomainUnload += (sender, e) => DisplayBufferedLogs();
    }
    
    public static void SetRawJsonMode(bool enabled)
    {
        _rawJsonMode = enabled;
    }

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
                    BufferKafkaConnectLog(trimmedLine);
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

    private static void BufferKafkaConnectLog(string logLine)
    {
        // Format the log for better readability
        var formattedLog = FormatKafkaConnectLog(logLine);
        BufferedLogs.Enqueue(formattedLog);
    }

    private static string FormatKafkaConnectLog(string logLine)
    {
        if (_rawJsonMode)
        {
            // Return raw JSON as is
            return logLine;
        }
        
        try
        {
            // Try to parse as JSON and extract the message
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
            // If not valid JSON, return as is with timestamp
            var timestamp = DateTime.Now.ToString("HH:mm:ss");
            return $"[{timestamp}] {logLine}";
        }
    }

    /// <summary>
    /// Display all buffered Kafka Connect logs at the end of test execution
    /// </summary>
    public static void DisplayBufferedLogs()
    {
        lock (Lock)
        {
            // Prevent double logging
            if (_logsDisplayed || BufferedLogs.IsEmpty)
                return;

            _logsDisplayed = true;

            Console.WriteLine();
            Console.WriteLine("========== KAFKA CONNECT LOGS ==========");
            Console.WriteLine();

            while (BufferedLogs.TryDequeue(out var log))
            {
                Console.WriteLine(log);
            }

            Console.WriteLine();
            Console.WriteLine("========================================");
            Console.WriteLine();
        }
    }

    /// <summary>
    /// Clear all buffered logs
    /// </summary>
    public static void ClearBuffer()
    {
        lock (Lock)
        {
            while (BufferedLogs.TryDequeue(out _)) { }
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            Flush();
            _buffer.Dispose();
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}