using System.Collections.Concurrent;
using System.Text;
using System.Text.RegularExpressions;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

/// <summary>
/// Buffers XUnit test output and displays it after all tests are complete
/// Captures test results and suppresses them during execution
/// </summary>
public class XUnitBufferedLogger : IDisposable
{
    private readonly ConcurrentQueue<string> _bufferedOutput = new();
    private readonly object _lock = new();
    private volatile bool _bufferingEnabled = false;
    private bool _disposed = false;
    
    // Patterns for identifying XUnit messages
    private static readonly Regex TestResultPattern = new(@"^\s*(Passed|Failed|Skipped)\s+.*\[\d+(\.\d+)?\s*(s|ms)\]", RegexOptions.Compiled);
    private static readonly Regex XUnitFrameworkPattern = new(@"^\s*\[xUnit\.net.*\]", RegexOptions.Compiled);

    public void StartBuffering()
    {
        _bufferingEnabled = true;
    }

    public void StopBuffering()
    {
        _bufferingEnabled = false;
    }

    public void BufferMessage(string message)
    {
        if (string.IsNullOrEmpty(message) || _disposed)
            return;

        // Check if this is a test result message
        if (IsTestResultMessage(message))
        {
            // Parse and store the test result, but don't buffer the output
            TestResultCollector.ParseAndAddResult(message);
            return;
        }

        // Check if this is an XUnit framework message
        if (IsXUnitFrameworkMessage(message))
        {
            // Suppress XUnit framework messages during execution
            return;
        }

        // Buffer all other XUnit output if buffering is enabled
        if (_bufferingEnabled)
        {
            _bufferedOutput.Enqueue(message);
        }
    }

    public void FlushBufferedLogs()
    {
        lock (_lock)
        {
            if (_bufferedOutput.IsEmpty)
                return;

            Console.WriteLine();
            Console.WriteLine("========== XUNIT TEST OUTPUT ==========");
            Console.WriteLine();

            while (_bufferedOutput.TryDequeue(out var output))
            {
                Console.Write(output);
            }

            Console.WriteLine();
            Console.WriteLine("=======================================");
            Console.WriteLine();
        }
    }

    public void Clear()
    {
        lock (_lock)
        {
            while (_bufferedOutput.TryDequeue(out _)) { }
        }
    }

    private bool IsTestResultMessage(string message)
    {
        return TestResultPattern.IsMatch(message);
    }

    private bool IsXUnitFrameworkMessage(string message)
    {
        return XUnitFrameworkPattern.IsMatch(message);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            FlushBufferedLogs();
            Clear();
            _disposed = true;
        }
    }
}

/// <summary>
/// Console writer that routes messages to the appropriate logger based on current phase
/// </summary>
public class PhaseAwareConsoleWriter : TextWriter
{
    private readonly TestLoggingCoordinator _coordinator;
    private readonly StringBuilder _lineBuffer = new();

    public PhaseAwareConsoleWriter(TestLoggingCoordinator coordinator)
    {
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
    }

    public override Encoding Encoding => Encoding.UTF8;

    public override void Write(char value)
    {
        _lineBuffer.Append(value);
        
        if (value == '\n')
        {
            ProcessLine();
        }
    }

    public override void Write(string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            _lineBuffer.Append(value);
            
            if (value.Contains('\n'))
            {
                ProcessLine();
            }
        }
    }

    public override void WriteLine(string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            _lineBuffer.Append(value);
        }
        _lineBuffer.AppendLine();
        ProcessLine();
    }

    private void ProcessLine()
    {
        var line = _lineBuffer.ToString();
        _lineBuffer.Clear();

        if (string.IsNullOrWhiteSpace(line))
        {
            return;
        }

        var trimmedLine = line.TrimEnd('\r', '\n');
        var source = DetermineLogSource(trimmedLine);
        
        _coordinator.LogMessage(trimmedLine, source);
    }

    private LogSource DetermineLogSource(string message)
    {
        if (message.Contains("[testcontainers.org"))
            return LogSource.TestContainers;
            
        if (message.Contains("|rdkafka#") || message.Contains("| [thrd:") || 
            message.Contains("|PARTCNT|") || message.StartsWith("%"))
            return LogSource.TestContainers;
            
        if (TestResultPattern.IsMatch(message) || XUnitFrameworkPattern.IsMatch(message))
            return LogSource.XUnit;
            
        // Check if it's a JSON log from Kafka Connect
        if (message.TrimStart().StartsWith("{") && message.TrimEnd().EndsWith("}"))
        {
            try
            {
                var jsonDoc = System.Text.Json.JsonDocument.Parse(message);
                if (jsonDoc.RootElement.TryGetProperty("Properties", out var props) &&
                    props.TryGetProperty("Log", out _))
                {
                    return LogSource.KafkaConnect;
                }
            }
            catch
            {
                // Not valid JSON, treat as infrastructure
            }
        }
            
        return LogSource.Infrastructure;
    }

    // Patterns for identifying different log sources
    private static readonly Regex TestResultPattern = new(@"^\s*(Passed|Failed|Skipped)\s+.*\[\d+(\.\d+)?\s*(s|ms)\]", RegexOptions.Compiled);
    private static readonly Regex XUnitFrameworkPattern = new(@"^\s*\[xUnit\.net.*\]", RegexOptions.Compiled);

    public override void Flush()
    {
        if (_lineBuffer.Length > 0)
        {
            ProcessLine();
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Flush();
        }
        base.Dispose(disposing);
    }
}