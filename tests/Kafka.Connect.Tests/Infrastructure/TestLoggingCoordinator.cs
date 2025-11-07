using System.Collections.Concurrent;
using System.Text;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

/// <summary>
/// Coordinates logging across different phases of test execution
/// </summary>
public class TestLoggingCoordinator : IDisposable
{
    private readonly TextWriter _originalOut;
    private readonly TextWriter _originalError;
    private LoggingPhase _currentPhase = LoggingPhase.InfrastructureSetup;
    private readonly object _phaseLock = new();
    
    // Phase-specific loggers
    private readonly InfrastructureLogger _infrastructureLogger;
    private readonly KafkaConnectStreamLogger _kafkaConnectLogger;
    private readonly XUnitBufferedLogger _xunitLogger;
    
    private bool _disposed = false;

    public TestLoggingCoordinator()
    {
        _originalOut = Console.Out;
        _originalError = Console.Error;
        
        _infrastructureLogger = new InfrastructureLogger(_originalOut);
        _kafkaConnectLogger = new KafkaConnectStreamLogger(_originalOut);
        _xunitLogger = new XUnitBufferedLogger();
        
        SetupConsoleRedirection();
    }

    public LoggingPhase CurrentPhase 
    { 
        get 
        { 
            lock (_phaseLock) 
            { 
                return _currentPhase; 
            } 
        } 
    }

    public void TransitionToPhase(LoggingPhase phase)
    {
        lock (_phaseLock)
        {
            if (_currentPhase == phase) return;
            
            var previousPhase = _currentPhase;
            _currentPhase = phase;
            
            LogPhaseTransition(previousPhase, phase);
            
            // Handle phase-specific transitions
            switch (phase)
            {
                case LoggingPhase.KafkaConnectStreaming:
                    _kafkaConnectLogger.StartStreaming();
                    break;
                case LoggingPhase.XUnitBuffering:
                    _xunitLogger.StartBuffering();
                    break;
                case LoggingPhase.InfrastructureCleanup:
                    _xunitLogger.FlushBufferedLogs();
                    break;
            }
        }
    }

    public void LogMessage(string message, LogSource source = LogSource.Infrastructure)
    {
        lock (_phaseLock)
        {
            switch (_currentPhase)
            {
                case LoggingPhase.InfrastructureSetup:
                case LoggingPhase.InfrastructureCleanup:
                    _infrastructureLogger.LogMessage(message, source);
                    break;
                    
                case LoggingPhase.KafkaConnectStreaming:
                    if (source == LogSource.KafkaConnect)
                        _kafkaConnectLogger.LogMessage(message);
                    else
                        _infrastructureLogger.LogMessage(message, source);
                    break;
                    
                case LoggingPhase.XUnitBuffering:
                    if (source == LogSource.XUnit)
                        _xunitLogger.BufferMessage(message);
                    else if (source == LogSource.KafkaConnect)
                        _kafkaConnectLogger.LogMessage(message);
                    else
                        _infrastructureLogger.LogMessage(message, source);
                    break;
            }
        }
    }

    public KafkaConnectStreamLogger GetKafkaConnectLogger()
    {
        return _kafkaConnectLogger;
    }

    private void SetupConsoleRedirection()
    {
        var redirector = new PhaseAwareConsoleWriter(this);
        Console.SetOut(redirector);
        Console.SetError(redirector);
    }

    private void LogPhaseTransition(LoggingPhase from, LoggingPhase to)
    {
        var timestamp = DateTime.Now.ToString("HH:mm:ss");
        var message = $"[{timestamp}] ========== LOGGING PHASE TRANSITION: {from} -> {to} ==========";
        _originalOut.WriteLine(message);
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        lock (_phaseLock)
        {
            // Ensure all buffered logs are flushed
            _xunitLogger.FlushBufferedLogs();
            
            // Restore original console
            Console.SetOut(_originalOut);
            Console.SetError(_originalError);
            
            _infrastructureLogger?.Dispose();
            _kafkaConnectLogger?.Dispose();
            _xunitLogger?.Dispose();
            
            _disposed = true;
        }
    }
}

/// <summary>
/// Defines the different phases of test logging
/// </summary>
public enum LoggingPhase
{
    InfrastructureSetup,
    KafkaConnectStreaming,
    XUnitBuffering,
    InfrastructureCleanup
}

/// <summary>
/// Identifies the source of log messages
/// </summary>
public enum LogSource
{
    Infrastructure,
    TestContainers,
    KafkaConnect,
    XUnit,
    Custom
}