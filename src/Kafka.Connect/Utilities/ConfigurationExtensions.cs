using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Plugin;
using Microsoft.Extensions.Configuration;
using Serilog;
using Serilog.Context;

namespace Kafka.Connect.Utilities
{
    public static class ConfigurationExtensions
    {
        public static void LoadPlugins(this IConfiguration configuration)
        {
            var plugins = configuration.GetSection("worker:plugins").Get<IEnumerable<PluginConfig>>();
             if (plugins == null)
             {
                 Log.ForContext<Worker>().Debug("{@Log}", new {Message = "No plugins registered. Please verify the configuration."});
                 return;
             }
             foreach (var plugin in plugins)
             {
                 using (LogContext.PushProperty("Plugin", plugin?.Name))
                 {
                     if (plugin?.Initializers == null || !plugin.Initializers.Any()) continue;
                     foreach (var initializer in plugin.Initializers)
                     {
                         if (!(initializer.Assembly?.EndsWith(".dll") ?? true))
                         {
                             initializer.Assembly = $"{initializer.Assembly}.dll";
                         }

                         Log.ForContext<Worker>().Verbose("{@Log}", new {Message = $"Loading Plugin: {plugin.Name}."});
                         var pluginLocation =
                             $"{AppDomain.CurrentDomain.BaseDirectory}{plugin.Directory}{Path.DirectorySeparatorChar}{initializer.Assembly}";

                         if (!File.Exists(pluginLocation))
                         {
                             Log.ForContext<Worker>().Warning("{@Log}", new {Message = $"No assembly found at {plugin.Directory}"});
                             continue;
                         }

                         var loadContext = new PluginLoadContext(pluginLocation);
                         var assembly = loadContext.LoadFromAssemblyName(AssemblyName.GetAssemblyName(pluginLocation));

                         var type = assembly.GetType(initializer.Type);
                         if (type != null && Activator.CreateInstance(type) is IPluginInitializer instance)
                         {
                             ServiceExtensions.AddPluginServices +=
                                 collection => instance.AddServices(collection, configuration, plugin.Name);
                         }
                         else
                         {
                             Log.ForContext<Worker>().Warning( "{@Log}", new { Message = $"Failed to instantiate the initializer: {initializer.Type}"});
                             continue;
                         }
                         Log.ForContext<Worker>().Debug("{@Log}", new {Message = $"Plugin Initialized: {plugin.Name}({initializer.Type})."});
                     }
                 }
             }
        }
    }
}