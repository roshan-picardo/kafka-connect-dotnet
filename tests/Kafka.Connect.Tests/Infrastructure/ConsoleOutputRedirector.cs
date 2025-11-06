using System.Text;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class ConsoleOutputRedirector : TextWriter
{
    private readonly TextWriter _originalOut;
    private readonly StringBuilder _capturedOutput = new();

    public ConsoleOutputRedirector(TextWriter originalOut)
    {
        _originalOut = originalOut;
    }

    public override Encoding Encoding => _originalOut.Encoding;

    public override void Write(char value)
    {
        _capturedOutput.Append(value);
        _originalOut.Write(value);
    }

    public override void Write(string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            _capturedOutput.Append(value);
            
            // Check if this is a test result line and capture it
            if (TestResultCollector.IsTestResultMessage(value))
            {
                TestResultCollector.ParseAndAddResult(value);
                // Don't output test results to console during execution
                return;
            }
        }
        
        _originalOut.Write(value);
    }

    public override void WriteLine(string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            _capturedOutput.AppendLine(value);
            
            // Check if this is a test result line and capture it
            if (TestResultCollector.IsTestResultMessage(value))
            {
                TestResultCollector.ParseAndAddResult(value);
                // Don't output test results to console during execution - suppress completely
                return;
            }
        }
        
        _originalOut.WriteLine(value);
    }

    public string GetCapturedOutput() => _capturedOutput.ToString();
}