using System.Net;
using Cassandra;
using DotNet.Testcontainers.Networks;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class CassandraFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network,
    TestCaseConfig[]? testConfigs)
    : DatabaseFixture(configuration, logMessage, containerService, network, testConfigs)
{
    // Cassandra takes much longer than other databases (JVM startup can be 60+ seconds under resource contention)
    private const int CassandraReadyMaxAttempts = 240; // 4 minutes total
    private const int CassandraInitialDelayMs = 15000; // Give container and JVM time to initialize
    private const int CassandraReadyDelayMs = 1000;

    private ICluster? _cluster;
    private ISession? _session;

    protected override string GetTargetName() => "cassandra";

    protected override async Task WaitForReadyAsync()
    {
        LogMessage($"Starting: {GetTargetName()} - waiting for JVM initialization...", "");
        await Task.Delay(CassandraInitialDelayMs);

        for (var attempt = 1; attempt <= CassandraReadyMaxAttempts; attempt++)
        {
            try
            {
                var session = GetSession();
                await session.ExecuteAsync(new SimpleStatement("SELECT release_version FROM system.local"));

                LogMessage($"Started: {GetTargetName()} after {attempt} attempts ({(attempt * CassandraReadyDelayMs + CassandraInitialDelayMs) / 1000}s)", "");
                return;
            }
            catch (Exception ex) when (attempt < CassandraReadyMaxAttempts)
            {
                // Session may be stale if the cluster reconnected to the wrong address — recreate it
                ResetSession();
                var errorType = ex.GetType().Name;
                var errorMsg = ex.Message.Length > 100 ? ex.Message[..100] : ex.Message;
                LogMessage($"Starting: {GetTargetName()} (attempt {attempt}/{CassandraReadyMaxAttempts}) - {errorType}: {errorMsg}", "");
                await Task.Delay(CassandraReadyDelayMs);
            }
            catch (Exception ex)
            {
                throw new TimeoutException($"Failed to start {GetTargetName()} after {CassandraReadyMaxAttempts} attempts ({(CassandraReadyMaxAttempts * CassandraReadyDelayMs + CassandraInitialDelayMs) / 1000}s elapsed)", ex);
            }
        }
    }

    protected override async Task ExecuteScriptsAsync(string database, string[] scripts)
    {
        var session = GetSession();
        foreach (var script in scripts)
        {
            await session.ExecuteAsync(new SimpleStatement(script));
        }
    }

    public ISession GetSession()
    {
        if (_session != null) return _session;

        var endpoint = Configuration.GetServiceEndpoint("Cassandra") ?? "127.0.0.1:9042";
        var hostAndPort = endpoint.Split(':', StringSplitOptions.RemoveEmptyEntries);
        var host = hostAndPort[0];
        var port = hostAndPort.Length > 1 && int.TryParse(hostAndPort[1], out var p) ? p : 9042;

        // The Cassandra driver rewrites contact points to whatever the node advertises (its Docker-internal
        // address). WithAddressTranslator ensures all resolved addresses are mapped back to the host+port
        // that is actually reachable from outside the container network.
        _cluster = Cluster.Builder()
            .AddContactPoint(host)
            .WithPort(port)
            .WithAddressTranslator(new SingleEndpointTranslator(host, port))
            .Build();

        _session = _cluster.Connect();
        return _session;
    }

    private void ResetSession()
    {
        try { _session?.Dispose(); } catch { /* ignore */ }
        try { _cluster?.Dispose(); } catch { /* ignore */ }
        _session = null;
        _cluster = null;
    }

    public override async ValueTask DisposeAsync()
    {
        ResetSession();
        await base.DisposeAsync();
    }

    // Translates any address the Cassandra node advertises back to the single reachable endpoint.
    private sealed class SingleEndpointTranslator(string host, int port) : IAddressTranslator
    {
        private readonly IPEndPoint _endpoint = new(IPAddress.Parse(host == "localhost" ? "127.0.0.1" : host), port);

        public IPEndPoint Translate(IPEndPoint address) => _endpoint;
    }
}
