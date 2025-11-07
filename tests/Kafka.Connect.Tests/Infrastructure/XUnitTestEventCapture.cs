using System.Text;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

/// <summary>
/// Simple approach: Suppress test result output during execution
/// Use string-based detection instead of complex regex patterns
/// </summary>
public class SimpleOutputSuppressor : TextWriter
{
    private readonly TextWriter _originalOut;
    private readonly TestLoggingCoordinator _coordinator;
    private readonly StringBuilder _lineBuffer = new();

    public SimpleOutputSuppressor(TextWriter originalOut, TestLoggingCoordinator coordinator)
    {
        _originalOut = originalOut;
        _coordinator = coordinator;
    }

    public override Encoding Encoding => _originalOut.Encoding;

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
            _originalOut.Write(line);
            return;
        }

        var trimmedLine = line.TrimEnd('\r', '\n');

        // Check if it's a test result line and suppress it
        if (IsTestResultLine(trimmedLine))
        {
            // Parse and capture but don't display during execution
            TestResultCollector.ParseAndAddResult(trimmedLine);
            return; // Suppress the output
        }

        // Check if it's Kafka Connect JSON and route appropriately
        if (IsKafkaConnectLog(trimmedLine))
        {
            _coordinator.LogMessage(trimmedLine, LogSource.KafkaConnect);
            return;
        }

        // For all other output, write it normally
        _originalOut.Write(line);
    }

    private bool IsTestResultLine(string line)
    {
        var trimmed = line.Trim();
        
        // Simple string-based detection - much more reliable than regex
        if (trimmed.StartsWith("Passed ") ||
            trimmed.StartsWith("Failed ") ||
            trimmed.StartsWith("Skipped "))
        {
            // Additional check to ensure it has a duration bracket
            return trimmed.Contains("[") && (trimmed.Contains("ms]") || trimmed.Contains("s]"));
        }
        
        return false;
    }

    private bool IsKafkaConnectLog(string line)
    {
        return line.TrimStart().StartsWith("{\"Timestamp\"") && line.Contains("\"Level\":");
    }

    public override void Flush()
    {
        if (_lineBuffer.Length > 0)
        {
            ProcessLine();
        }
        _originalOut.Flush();
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