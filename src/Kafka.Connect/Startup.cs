using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Background;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Utilities;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Formatting.Json;

 namespace Kafka.Connect
 {
     internal static class Startup
     {
         private static CancellationTokenSource _cts;

         private static IHostBuilder ConfigureHostBuilder(string[] args, IConfiguration configuration) =>
             Host.CreateDefaultBuilder(args)
                 .ConfigureAppConfiguration(builder => builder.AddConfiguration(configuration))
                 .ConfigureLogging(logging => logging.ClearProviders())
                 .ConfigureWebHostDefaults(webBuilder => { webBuilder.UseStartup<ApiStartup>(); })
                 .ConfigureServices(collection =>
                 {
                     collection
                         .AddHostedService<LeaderService>()
                         .AddHostedService<WorkerService>()
                         .AddHostedService<HealthCheckService>();
                         //.AddHostedService<FailOverMonitorService>();
                 });


         private static IConfiguration LoadConfiguration(Arguments args)
         {
             var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "loc";
             var builder = new ConfigurationBuilder()
                 .SetBasePath(Directory.GetCurrentDirectory());
             builder.AddJsonFile("appsettings.json", true, true);
             builder.AddJsonFile($"appsettings.{environment}.json", true, true);
             if (!args.TryGetValue("config", out var files)) return builder.Build();
             files.ForEach(file => builder.AddJsonFile(string.Format(file, environment), false, true));
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

                 Log.ForContext<ConnectLog>().Information("{@Log}", new {Message = "Kafka Connect starting..."});

                 // time this
                 var configuration = LoadConfiguration(Arguments.Parse(args));
                 // time this
                 configuration.LoadPlugins();
                 
                 AppDomain.CurrentDomain.ProcessExit += (_, _) => { _cts?.Cancel(); };

                 Log.ForContext<Worker>().Verbose("{@Log}", new {Message = "Initializing the web host."});

                 var host = ConfigureHostBuilder(args, configuration).Build();
                 var executionContext = host.Services.GetService<IExecutionContext>();
                 _cts = executionContext?.GetToken() ?? new CancellationTokenSource();
                 _cts.Token.Register(() => Log.ForContext<ConnectLog>().Information("{@Log}",new {Message = "Shutting down Kafka Connect."}));
                 
                 Console.CancelKeyPress += (_, eventArgs) =>
                 {
                     _cts.Cancel();
                     eventArgs.Cancel = true;
                 };

                 await host.RunAsync(_cts.Token).ContinueWith(t =>
                 {
                     if (!t.IsFaulted)
                     {
                         while (!(executionContext?.IsStopped ?? true))
                         {
                             Log.ForContext<Worker>().Information("{@Log}",
                                 new
                                 {
                                     Message = "Shutting down Kafka Connect."
                                 });
                             Thread.Sleep(5000);
                         }

                         return;
                     }
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
                                 Message = "Kafka Connect failed to start...",
                                 Reason = t.Exception?.InnerException?.Message
                             });
                     }
                 });
             }
             catch (Exception ex)
             {
                 Log.ForContext<Worker>().Fatal(ex, "{@Log}", new {Message = "Kafka Connect failed to start...", Reason = ex.Message});
             }
             finally
             {
                 Log.ForContext<ConnectLog>().Information("{@Log}", new {Message = "Kafka Connect shutdown successfully..."});
             }
         }
     }
 }