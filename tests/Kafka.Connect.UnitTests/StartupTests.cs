using System;
using System.IO;
using System.Linq;
using System.Reflection;
using Kafka.Connect;
using Kafka.Connect.Utilities;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace UnitTests.Kafka.Connect;

public class StartupTests
{
    [Fact]
    public void LoadConfiguration_LoadsSettingsFolderAndConfigFiles()
    {
        var startupType = typeof(Worker).Assembly.GetType("Kafka.Connect.Startup");
        Assert.NotNull(startupType);

        var tempDir = Path.Combine(Path.GetTempPath(), "kafka-connect-startup-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var settingsFile = Path.Combine(tempDir, "01.settings.json");
            var configFile = Path.Combine(tempDir, "custom.json");

            File.WriteAllText(settingsFile, """
            {
              "fromSettings": { "value": "settings-ok" }
            }
            """);

            File.WriteAllText(configFile, """
            {
              "fromConfig": { "value": "config-ok" }
            }
            """);

            var args = Arguments.Parse([$"--settings={tempDir}", $"--config={configFile}"]);

            var loadConfiguration = startupType!.GetMethod("LoadConfiguration", BindingFlags.NonPublic | BindingFlags.Static);
            Assert.NotNull(loadConfiguration);

            var configuration = (IConfiguration)loadConfiguration!.Invoke(null, [args])!;

            Assert.Equal("settings-ok", configuration["fromSettings:value"]);
            Assert.Equal("config-ok", configuration["fromConfig:value"]);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Fact]
    public void ConfigureHostBuilder_RegistersKafkaHostedServices()
    {
        var startupType = typeof(Worker).Assembly.GetType("Kafka.Connect.Startup");
        Assert.NotNull(startupType);

        var configureHostBuilder = startupType!.GetMethod("ConfigureHostBuilder", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(configureHostBuilder);

        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();
        var hostBuilder = (IHostBuilder)configureHostBuilder!.Invoke(null, [Array.Empty<string>(), configuration])!;

        using var host = hostBuilder.Build();
        var hostedServices = host.Services.GetServices<IHostedService>().ToList();

        Assert.Contains(hostedServices, s => s.GetType().Name == "LeaderService");
        Assert.Contains(hostedServices, s => s.GetType().Name == "WorkerService");
        Assert.Contains(hostedServices, s => s.GetType().Name == "HealthCheckService");
        Assert.Contains(hostedServices, s => s.GetType().Name == "FailOverMonitorService");
    }
}
