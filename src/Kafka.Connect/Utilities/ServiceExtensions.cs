using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Connect.Builders;
using Kafka.Connect.Connectors;
using Kafka.Connect.Converters;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Processors;
using Kafka.Connect.Serializers;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Linq;
using Serilog;
using Kafka.Connect.Configurations;
using Kafka.Connect.Logging;
using Kafka.Connect.Plugin.Logging;

namespace Kafka.Connect.Utilities
{
    internal static class ServiceExtensions
    {
        internal static Action<IServiceCollection> AddPluginServices;

        internal static IServiceCollection AddServices(this IServiceCollection services, IConfiguration configuration)
        {
            var trace = configuration.GetSection("worker").Get<WorkerConfig>()?.Trace ?? false;
            services
                .AddScoped(typeof(ILogger<>), typeof(Logger<>))
                .AddSingleton<ILogDecorator, LogDecorator>()
                .AddScoped<IConnector, Connector>()
                .AddScoped<ISinkTask, SinkTask>()
                .AddScopedWithLogging<IKafkaClientBuilder, KafkaClientBuilder>(trace)
                .AddScoped<IKafkaClientEventHandler, KafkaClientEventHandler>()
                .AddScoped<IRetriableHandler, RetriableHandler>()

                .AddScopedWithLogging<IConnectDeadLetter, ConnectDeadLetter>(trace)
                .AddScoped<ITokenHandler, TokenHandler>()

                .AddScoped<IProcessorServiceProvider, ProcessorServiceProvider>()
                .AddScoped<ISinkHandlerProvider, SinkHandlerProvider>()

                .AddScopedWithLogging<IGenericRecordParser, GenericRecordParser>(trace)
                .AddScopedWithLogging<IRecordFlattener, JsonRecordFlattener>(trace)
                .AddScopedWithLogging<IMessageHandler, MessageHandler>(trace)
                .AddScopedWithLogging<ISinkConsumer, SinkConsumer>(trace)
                .AddScopedWithLogging<ISinkProcessor, SinkProcessor>(trace)
                .AddScopedWithLogging<IPartitionHandler, PartitionHandler>(trace)
                .AddScopedWithLogging<ISinkExceptionHandler, SinkExceptionHandler>(trace)

                .AddScopedWithLogging<IProcessor, JsonTypeOverrider>(trace)
                .AddScopedWithLogging<IProcessor, DateTimeTypeOverrider>(trace)
                .AddScopedWithLogging<IProcessor, BlacklistFieldProjector>(trace)
                .AddScopedWithLogging<IProcessor, WhitelistFieldProjector>(trace)
                .AddScopedWithLogging<IProcessor, FieldRenamer>()
                
                .AddScoped<IAsyncDeserializer<GenericRecord>, AvroDeserializer<GenericRecord>>()
                .AddScoped<IAsyncDeserializer<JObject>, AvroDeserializer<JObject>>()
                .AddScoped<ISchemaRegistryClient>(_ =>
                {
                    var config = configuration.GetSection("worker:schemaRegistry").Get<SchemaRegistryConfig>();
                    return string.IsNullOrEmpty(config?.Url) ? null : new CachedSchemaRegistryClient(config);
                })
                .AddScopedWithLogging<IDeserializer, AvroDeserializer>(trace)
                .AddScopedWithLogging<IDeserializer, JsonDeserializer>(trace)
                .AddScopedWithLogging<IDeserializer, JsonSchemaDeserializer>(trace)
                .AddScopedWithLogging<IDeserializer, StringDeserializer>(trace)
                .AddScopedWithLogging<IDeserializer, IgnoreDeserializer>(trace)
                .AddScopedWithLogging<IMessageConverter, MessageConverter>(trace)

                .Configure<WorkerConfig>(configuration.GetSection("worker"), options => options.BindNonPublicProperties = true)
                
                .AddSingleton<Providers.IConfigurationProvider, Providers.ConfigurationProvider>()
                .AddSingleton<Plugin.Providers.IConfigurationProvider, Providers.ConfigurationProvider>()
                .AddSingleton<IExecutionContext, ExecutionContext>()
                .AddSingleton<IWorker, Worker>()
                .AddControllers();

            AddPluginServices?.Invoke(services);
            return services;
        }

        internal static IServiceCollection AddLogger(this IServiceCollection services, IConfiguration configuration)
        {
            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(configuration)
                .AddDefaultEnrichers()
                .Destructure.With<JTokenDestructurePolicy>()
                .CreateLogger();
            return services;
        }
        
        internal static LoggerConfiguration AddDefaultEnrichers(this LoggerConfiguration logger)
        {
            return logger.Enrich.WithProperty("Versions", new Dictionary<string, string>
                {
                    {"net", Environment.Version.ToString()},
                    {"lib", Library.VersionString},
                    {"base", Assembly.GetExecutingAssembly().GetName().Version?.ToString()},
                    {"app", Environment.GetEnvironmentVariable("APPLICATION_VERSION") ?? "0.0.0.0"}
                })
                .Enrich.WithProperty("Environment", Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"))
                .Enrich.WithProperty("Worker",
                    Environment.GetEnvironmentVariable("WORKER_HOST") ?? Environment.MachineName);
        }
    }
}