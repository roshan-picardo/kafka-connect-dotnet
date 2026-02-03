using System.Collections.Concurrent;
using System.Text.Json;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class KafkaConnectLogBuffer : Stream
{
    private readonly MemoryStream _buffer = new();
    private static readonly ConcurrentQueue<string> BufferedLogs = new();
    private static readonly object Lock = new();
    private static bool _logsDisplayed;
    private static bool _rawJsonMode;
    private static bool _skipLogFlush;
    private bool _disposed;
    
    static KafkaConnectLogBuffer()
    {
        AppDomain.CurrentDomain.ProcessExit += (_, _) => DisplayBufferedLogs();
        AppDomain.CurrentDomain.DomainUnload += (_, _) => DisplayBufferedLogs();
    }
    
    public static void SetRawJsonMode(bool enabled)
    {
        _rawJsonMode = enabled;
    }
    
    public static void SetSkipLogFlush(bool skip)
    {
        _skipLogFlush = skip;
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
        var formattedLog = FormatKafkaConnectLog(logLine);
        BufferedLogs.Enqueue(formattedLog);
    }

    private static string FormatKafkaConnectLog(string logLine)
    {
        if (_rawJsonMode)
        {
            return logLine;
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

    private static void DisplayBufferedLogs()
    {
        lock (Lock)
        {
            if (_skipLogFlush || _logsDisplayed || BufferedLogs.IsEmpty)
                return;

            _logsDisplayed = true;

            Console.WriteLine(" ");

            while (BufferedLogs.TryDequeue(out var log))
            {
                Console.WriteLine(log);
            }

            Console.WriteLine(" ");
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