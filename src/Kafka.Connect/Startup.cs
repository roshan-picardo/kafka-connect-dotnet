﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
 using Kafka.Connect.Background;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Utilities;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Context;
 using Serilog.Formatting.Json;

 namespace Kafka.Connect
 {
     internal static class Startup
     {
         private static CancellationTokenSource _cts;

         private static void LoadPlugins(IConfiguration configuration)
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

         private static IHostBuilder ConfigureHostBuilder(string[] args, IConfiguration configuration) =>
             Host.CreateDefaultBuilder(args)
                 .ConfigureAppConfiguration(builder => builder.AddConfiguration(configuration))
                 .ConfigureLogging(logging => logging.ClearProviders())
                 .ConfigureWebHostDefaults(webBuilder => { webBuilder.UseStartup<ApiStartup>(); })
                 .ConfigureServices(collection =>
                 {
                     collection
                         .AddHostedService<WorkerService>()
                         .AddHostedService<HealthCheckService>()
                         .AddHostedService<FailOverMonitorService>();
                 });


         private static IConfiguration LoadConfiguration(Arguments args)
         {
             var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "loc";
             var builder = new ConfigurationBuilder()
                 .SetBasePath(Directory.GetCurrentDirectory());
             builder.AddJsonFile("appsettings.json", true, true);
             builder.AddJsonFile($"appsettings.{environment}.json", true, true);
             if (!args.TryGetValue("config", out var files)) return builder.Build();
             foreach (var file in files)
             {
                 builder.AddJsonFile(string.Format(file, environment), false, true);
             }
             return builder.Build();
         }

         private static async Task Main(string[] args)
         {
             try
             {
                 Log.Logger = new LoggerConfiguration()
                     .MinimumLevel.Information()
                     .AddDefaultEnrichers()
                     .WriteTo.Console(new JsonFormatter())
                     .CreateLogger();

                 Log.ForContext<Worker>().Information("{@Log}", new {Message = "Kafka Connect starting..."});

                 // time this
                 var configuration = LoadConfiguration(Arguments.Parse(args));
                 // time this
                 LoadPlugins(configuration);
                 
                 _cts = new CancellationTokenSource();

                 AppDomain.CurrentDomain.ProcessExit += (_, _) => { _cts.Cancel(); };

                 Console.CancelKeyPress += (_, eventArgs) =>
                 {
                     Log.ForContext<Worker>().Debug("{@Log}", new {Message = "Worker shutdown initiated."});
                     _cts.Cancel();
                     eventArgs.Cancel = true;
                 };

                 Log.ForContext<Worker>().Verbose("{@Log}", new {Message = "Initializing the web host."});

                 var host = ConfigureHostBuilder(args, configuration).Build();

                 await host.RunAsync(_cts.Token).ContinueWith(t =>
                 {
                     if (!t.IsFaulted) return;
                     if (t.Exception?.InnerException is OperationCanceledException)
                     {

                         Log.ForContext<Worker>().Warning("{@Log}",
                             new
                             {
                                 Message = "Parent operation has been cancelled. Triggering the host to terminate."
                             });
                     }
                     else
                     {
                         Log.ForContext<Worker>().Fatal(t.Exception, "{@Log}",
                             new
                             {
                                 Message = "Kafka Connect failed start...",
                                 Reason = t.Exception?.InnerException?.Message
                             });
                     }
                 });
             }
             catch (Exception ex)
             {
                 Log.ForContext<Worker>().Fatal(ex, "{@Log}", new {Message = "Kafka Connect failed start...", Reason = ex.Message});
             }
             finally
             {
                 Log.ForContext<Worker>().Information("{@Log}", new {Message = "Kafka Connect shutdown successfully..."});
             }
         }
     }
 }