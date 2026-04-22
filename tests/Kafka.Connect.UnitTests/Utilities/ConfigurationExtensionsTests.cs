using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Kafka.Connect.Utilities;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace UnitTests.Kafka.Connect.Utilities;

public class ConfigurationExtensionsTests
{
    [Fact]
    public void ReloadConfigs_WhenFolderMissing_ReturnsConfigurationWithOriginalValues()
    {
        var original = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                ["worker:name"] = "worker-a"
            })
            .Build();

        var reloaded = original.ReloadConfigs("/tmp/does-not-exist-kafka-connect-tests");

        Assert.Equal("worker-a", reloaded["worker:name"]);
    }

    [Fact]
    public void ReloadConfigs_WhenJsonExists_LoadsOverrideValues()
    {
        var tempDir = CreateTempDirectory();
        try
        {
            File.WriteAllText(Path.Combine(tempDir, "override.json"), """
            {
              "worker": {
                "name": "worker-reloaded"
              }
            }
            """);

            var original = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    ["worker:name"] = "worker-original",
                    ["worker:bootstrapServers"] = "localhost:9092"
                })
                .Build();

            var reloaded = original.ReloadConfigs(tempDir);

            Assert.Equal("worker-reloaded", reloaded["worker:name"]);
            Assert.Equal("localhost:9092", reloaded["worker:bootstrapServers"]);
        }
        finally
        {
            Directory.Delete(tempDir, true);
        }
    }

    [Fact]
    public void LoadPlugins_WhenNoPluginSection_DoesNotThrowOrRegisterPluginHook()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                ["worker:name"] = "worker-a"
            })
            .Build();

        var original = GetAddPluginServices();
        try
        {
            SetAddPluginServices(null);
            config.LoadPlugins();
            Assert.Null(GetAddPluginServices());
        }
        finally
        {
            SetAddPluginServices(original);
        }
    }

    [Fact]
    public void LoadPlugins_WhenPluginDirectoryMissing_DoesNotThrow()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                ["worker:plugins:location"] = "/tmp/definitely-missing-kafka-connect-plugin-dir"
            })
            .Build();

        config.LoadPlugins();
    }

    [Fact]
    public void LoadPlugins_WhenAssemblyNotFound_ContinuesWithoutRegisteringHook()
    {
        var tempDir = CreateTempDirectory();
        try
        {
            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    ["worker:plugins:location"] = tempDir,
                    ["worker:plugins:initializers:plugin-a:folder"] = "plugin-a",
                    ["worker:plugins:initializers:plugin-a:assembly"] = "Missing.dll",
                    ["worker:plugins:initializers:plugin-a:class"] = "Plugin.Initializer"
                })
                .Build();

            var original = GetAddPluginServices();
            try
            {
                SetAddPluginServices(null);
                config.LoadPlugins();
                Assert.Null(GetAddPluginServices());
            }
            finally
            {
                SetAddPluginServices(original);
            }
        }
        finally
        {
            Directory.Delete(tempDir, true);
        }
    }

    [Fact]
    public void LoadPlugins_WhenMultipleAssembliesAmbiguous_ContinuesWithoutRegisteringHook()
    {
        var tempDir = CreateTempDirectory();
        try
        {
            Directory.CreateDirectory(Path.Combine(tempDir, "first"));
            Directory.CreateDirectory(Path.Combine(tempDir, "second"));
            File.WriteAllText(Path.Combine(tempDir, "first", "Plugin.dll"), string.Empty);
            File.WriteAllText(Path.Combine(tempDir, "second", "Plugin.dll"), string.Empty);

            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    ["worker:plugins:location"] = tempDir,
                    ["worker:plugins:initializers:plugin-a:folder"] = "plugin-a",
                    ["worker:plugins:initializers:plugin-a:assembly"] = "Plugin.dll",
                    ["worker:plugins:initializers:plugin-a:class"] = "Plugin.Initializer"
                })
                .Build();

            var original = GetAddPluginServices();
            try
            {
                SetAddPluginServices(null);
                config.LoadPlugins();
                Assert.Null(GetAddPluginServices());
            }
            finally
            {
                SetAddPluginServices(original);
            }
        }
        finally
        {
            Directory.Delete(tempDir, true);
        }
    }

    [Fact]
    public void LoadPlugins_WhenAssemblyLoadsButInitializerTypeMissing_DoesNotRegisterHook()
    {
        var tempDir = CreateTempDirectory();
        try
        {
            var sourceAssembly = typeof(global::Kafka.Connect.Utilities.ConfigurationExtensions).Assembly.Location;
            var assemblyName = Path.GetFileName(sourceAssembly);
            var pluginFolder = Path.Combine(tempDir, "plugin-a");
            Directory.CreateDirectory(pluginFolder);
            File.Copy(sourceAssembly, Path.Combine(pluginFolder, assemblyName), overwrite: true);

            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    ["worker:plugins:location"] = tempDir,
                    ["worker:plugins:initializers:plugin-a:folder"] = "plugin-a",
                    ["worker:plugins:initializers:plugin-a:assembly"] = assemblyName,
                    ["worker:plugins:initializers:plugin-a:class"] = "Missing.Initializer.Type"
                })
                .Build();

            var original = GetAddPluginServices();
            try
            {
                SetAddPluginServices(null);
                config.LoadPlugins();
                Assert.Null(GetAddPluginServices());
            }
            finally
            {
                SetAddPluginServices(original);
            }
        }
        finally
        {
            Directory.Delete(tempDir, true);
        }
    }

    [Fact]
    public void ApiStartup_ConfigureServices_RegistersCoreServicesAndInvokesPluginHook()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                ["worker:name"] = "worker-a",
                ["worker:bootstrapServers"] = "localhost:9092",
                ["worker:standalone"] = "true",
                ["worker:connectors:orders:name"] = "orders",
                ["worker:connectors:orders:plugin:name"] = "plugin-a",
                ["worker:connectors:orders:plugin:type"] = "Sink",
                ["worker:plugins:initializers:plugin-a:folder"] = "plugin-a",
                ["worker:plugins:initializers:plugin-a:assembly"] = "Plugin.dll",
                ["worker:plugins:initializers:plugin-a:class"] = "Plugin.Initializer"
            })
            .Build();

        var original = GetAddPluginServices();
        try
        {
            SetAddPluginServices(services => services.AddSingleton<MarkerService>());
            var startup = new ApiStartup(configuration);
            var services = new ServiceCollection();

            startup.ConfigureServices(services);

            Assert.Contains(services, s => s.ServiceType == typeof(global::Kafka.Connect.Providers.IConfigurationProvider));
            Assert.Contains(services, s => s.ServiceType == typeof(global::Kafka.Connect.Plugin.Providers.IConfigurationProvider));
            Assert.Contains(services, s => s.ServiceType == typeof(MarkerService));
        }
        finally
        {
            SetAddPluginServices(original);
        }
    }

    private static Action<IServiceCollection> GetAddPluginServices()
    {
        var field = typeof(ApiStartup).Assembly
            .GetType("Kafka.Connect.Utilities.ServiceExtensions")!
            .GetField("AddPluginServices", BindingFlags.Static | BindingFlags.NonPublic)!;
        return field.GetValue(null) as Action<IServiceCollection>;
    }

    private static void SetAddPluginServices(Action<IServiceCollection> value)
    {
        var field = typeof(ApiStartup).Assembly
            .GetType("Kafka.Connect.Utilities.ServiceExtensions")!
            .GetField("AddPluginServices", BindingFlags.Static | BindingFlags.NonPublic)!;
        field.SetValue(null, value);
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "kafka-connect-utilities-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class MarkerService;
}
