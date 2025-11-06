using System.Text;
using System.Text.RegularExpressions;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class XUnitOutputSuppressor : TextWriter
{
    private readonly TextWriter _originalWriter;
    private readonly StringBuilder _lineBuffer = new();
    private static readonly Regex TestResultPattern = new(@"^\s*(Passed|Failed|Skipped)\s+.*\[\d+(\.\d+)?\s*(s|ms)\]", RegexOptions.Compiled);
    private static readonly Regex XUnitFrameworkPattern = new(@"^\s*\[xUnit\.net.*\]", RegexOptions.Compiled);

    public XUnitOutputSuppressor(TextWriter originalWriter)
    {
        _originalWriter = originalWriter;
    }

    public override Encoding Encoding => _originalWriter.Encoding;

    public override void Write(char value)
    {
        _lineBuffer.Append(value);
        
        // Check if we have a complete line
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
            
            // Check if the string contains newlines
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
            _originalWriter.Write(line);
            return;
        }

        var trimmedLine = line.TrimEnd('\r', '\n');

        // Check if this is a test result line
        if (TestResultPattern.IsMatch(trimmedLine))
        {
            // Capture the test result and suppress output
            TestResultCollector.ParseAndAddResult(trimmedLine);
            return; // Don't write to original output
        }

        // Check if this is an XUnit framework message
        if (XUnitFrameworkPattern.IsMatch(trimmedLine))
        {
            return; // Suppress XUnit framework messages
        }

        // Write all other content to original output
        _originalWriter.Write(line);
    }

    public override void Flush()
    {
        if (_lineBuffer.Length > 0)
        {
            ProcessLine();
        }
        _originalWriter.Flush();
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