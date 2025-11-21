using System.Text;
using System.Text.RegularExpressions;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class XUnitOutputSuppressor(TextWriter originalWriter) : TextWriter
{
    private readonly StringBuilder _lineBuffer = new();
    private static readonly Regex TestResultPattern = new(@"^\s*(Passed|Failed|Skipped)\s+.*\[\d+(\.\d+)?\s*(s|ms)\]", RegexOptions.Compiled);
    private static readonly Regex XUnitFrameworkPattern = new(@"^\s*\[xUnit\.net.*\]", RegexOptions.Compiled);

    public override Encoding Encoding => originalWriter.Encoding;

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
            originalWriter.Write(line);
            return;
        }

        var trimmedLine = line.TrimEnd('\r', '\n');

        if (TestResultPattern.IsMatch(trimmedLine))
        {
            System.Diagnostics.Debug.WriteLine($"SUPPRESSED TEST RESULT: {trimmedLine}");
        }

        if (XUnitFrameworkPattern.IsMatch(trimmedLine))
        {
            System.Diagnostics.Debug.WriteLine($"SUPPRESSED XUNIT FRAMEWORK: {trimmedLine}");
        }

        originalWriter.Write(line);
    }

    public override void Flush()
    {
        if (_lineBuffer.Length > 0)
        {
            ProcessLine();
        }
        originalWriter.Flush();
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