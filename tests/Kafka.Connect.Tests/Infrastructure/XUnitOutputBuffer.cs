using System.Collections.Concurrent;
using System.Text;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

/// <summary>
/// Global buffer for all XUnit test output that gets displayed after infrastructure teardown
/// </summary>
public static class XUnitOutputBuffer
{
    private static readonly ConcurrentQueue<string> BufferedOutput = new();
    private static readonly object Lock = new();
    private static bool _bufferingEnabled = true;

    public static void AddOutput(string output)
    {
        if (_bufferingEnabled && !string.IsNullOrEmpty(output))
        {
            BufferedOutput.Enqueue(output);
        }
    }

    public static void DisplayAllBufferedOutput()
    {
        lock (Lock)
        {
            if (BufferedOutput.IsEmpty)
                return;

            Console.WriteLine();
            Console.WriteLine("========== XUNIT TEST OUTPUT ==========");
            Console.WriteLine();

            while (BufferedOutput.TryDequeue(out var output))
            {
                Console.Write(output);
            }

            Console.WriteLine();
            Console.WriteLine("=======================================");
            Console.WriteLine();
        }
    }

    public static void DisableBuffering()
    {
        _bufferingEnabled = false;
    }

    public static void EnableBuffering()
    {
        _bufferingEnabled = true;
    }

    public static void Clear()
    {
        lock (Lock)
        {
            while (BufferedOutput.TryDequeue(out _)) { }
        }
    }
}