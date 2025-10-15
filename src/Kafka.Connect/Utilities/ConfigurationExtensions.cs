using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace Kafka.Connect.Utilities;

public static class ConfigurationExtensions
{
    public static void LoadPlugins(this IConfiguration configuration)
    {
        var plugins = configuration.GetSection("worker:plugins").Get<PluginAssemblyConfig>();
        if (plugins == null)
        {
            Log.ForContext<Worker>().Debug("{@Log}", new {Message = "No plugins registered. Please verify the configuration."});
            return;
        }
        plugins.Location = Directory.Exists(plugins.Location) ? plugins.Location : $"{AppDomain.CurrentDomain.BaseDirectory}{plugins.Location}";
        if (!Directory.Exists(plugins.Location))
        {
            Log.ForContext<Worker>().Debug("{@Log}", new {Message = "Plugins directory is empty. Continuing without loading any plugins."});
            return;
        }
        foreach (var (name, initializer) in plugins.Initializers)
        {
            string pluginLocation = null;
            Log.ForContext<Worker>().Debug("{@Log}", new {Message = $"Loading plugin - {name}"});
            var assemblyFiles = Directory.EnumerateFiles(plugins.Location, $"*{initializer.Assembly}", SearchOption.AllDirectories).ToList();
            if (!assemblyFiles.Any())
            {
                Log.ForContext<Worker>().Warning("{@Log}", new {Message = $"Assembly not found. {initializer.Assembly}"});
                continue;
            }

            if (assemblyFiles.Count > 1)
            {
                // try locating based on prefix
                var prefixedAssemblyFiles = assemblyFiles.Where(af => af.EndsWith($"{initializer.Folder}{Path.DirectorySeparatorChar}{initializer.Assembly}")).ToList();
                if (!prefixedAssemblyFiles.Any() || prefixedAssemblyFiles.Count > 1)
                {
                    Log.ForContext<Worker>().Error("{@Log}", new {Message = $"More than one matching assembly found. {initializer.Assembly}:{assemblyFiles.Count}"});
                    continue;
                }

                pluginLocation = prefixedAssemblyFiles.Single();
            }

            pluginLocation ??= assemblyFiles.Single();
                 
            var loadContext = new PluginLoadContext(pluginLocation);
            var assembly = loadContext.LoadFromAssemblyName(AssemblyName.GetAssemblyName(pluginLocation));

            var type = assembly.GetType(initializer.Class);

            if (type != null && Activator.CreateInstance(type) is IPluginInitializer instance)
            {
                var connectors = configuration.GetSection("worker:connectors")
                    .Get<IDictionary<string, ConnectorConfig>>()?
                    .Where(c => c.Value.Plugin.Name == name && !c.Value.Disabled).Select(c => (c.Value.Name ?? c.Key, MaxTasks: c.Value.Tasks));
                if (connectors != null)
                {
                    ServiceExtensions.AddPluginServices +=
                        collection => instance.AddServices(collection, configuration, connectors.ToArray());
                }
            }
            else
            {
                Log.ForContext<Worker>().Warning( "{@Log}", new { Message = $"Failed to instantiate the initializer: {initializer.Class}"});
                continue;
            }
            Log.ForContext<Worker>().Debug("{@Log}", new {Message = $"Plugin Initialized: {name}."});
        }
    }

    public static IConfiguration ReloadConfigs(this IConfiguration configuration, string folder = null)
    {
        var builder = new ConfigurationBuilder()
            .AddConfiguration(configuration);

        foreach (var file in Directory.EnumerateFiles(folder ?? Directory.GetCurrentDirectory(), "*.json"))
        {
            builder.AddJsonFile(file, true, true);
        }    
        var configurationRoot = builder.Build();
        configurationRoot.Reload();
        return configurationRoot;
    }
}