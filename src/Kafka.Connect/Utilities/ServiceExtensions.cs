using System;
using System.Collections.Generic;
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
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Strategies;

namespace Kafka.Connect.Utilities
{
    internal static class ServiceExtensions
    {
        internal static Action<IServiceCollection> AddPluginServices;

        internal static IServiceCollection AddServices(this IServiceCollection services, IConfiguration configuration)
        {
            services
                .AddScoped(typeof(ILogger<>), typeof(Logger<>))
                .AddScoped<IConnector, Connector>()
                .AddScoped<ISinkTask, SinkTask>()
                .AddScoped<ISourceTask, SourceTask>()
                .AddScoped<IKafkaClientBuilder, KafkaClientBuilder>()
                .AddScoped<IKafkaClientEventHandler, KafkaClientEventHandler>()
                .AddScoped<IRetriableHandler, RetriableHandler>()
                
                .AddScoped<ISourceHandler, SourceHandler>() //TODO: requires a provider

                .AddScoped<IConnectDeadLetter, ConnectDeadLetter>()
                .AddScoped<ITokenHandler, TokenHandler>()

                .AddScoped<IProcessorServiceProvider, ProcessorServiceProvider>()
                .AddScoped<ISinkHandlerProvider, SinkHandlerProvider>()

                .AddScoped<IGenericRecordParser, GenericRecordParser>()
                .AddScoped<IRecordFlattener, JsonRecordFlattener>()
                .AddScoped<IMessageHandler, MessageHandler>()
                .AddScoped<ISinkConsumer, SinkConsumer>()
                .AddScoped<ISourceProducer, SourceProducer>()
                .AddScoped<ISinkProcessor, SinkProcessor>()
                .AddScoped<IPartitionHandler, PartitionHandler>()
                .AddScoped<ISinkExceptionHandler, SinkExceptionHandler>()

                .AddScoped<IProcessor, JsonTypeOverrider>()
                .AddScoped<IProcessor, DateTimeTypeOverrider>()
                .AddScoped<IProcessor, BlacklistFieldProjector>()
                .AddScoped<IProcessor, WhitelistFieldProjector>()
                .AddScoped<IProcessor, FieldRenamer>()
                .AddScoped<ILogRecord, DefaultLogRecord>()
                
                .AddScoped<IAsyncDeserializer<GenericRecord>, AvroDeserializer<GenericRecord>>()
                .AddScoped<IAsyncDeserializer<JObject>, AvroDeserializer<JObject>>()
                .AddScoped<ISchemaRegistryClient>(_ =>
                {
                    var config = configuration.GetSection("worker:schemaRegistry").Get<SchemaRegistryConfig>();
                    return string.IsNullOrEmpty(config?.Url) ? null : new CachedSchemaRegistryClient(config);
                })
                .AddScoped<IDeserializer, AvroDeserializer>()
                .AddScoped<IDeserializer, JsonDeserializer>()
                .AddScoped<IDeserializer, JsonSchemaDeserializer>()
                .AddScoped<IDeserializer, StringDeserializer>()
                .AddScoped<IDeserializer, IgnoreDeserializer>()
                .AddScoped<IMessageConverter, MessageConverter>()
                
                .AddScoped<IWriteStrategyProvider, WriteStrategyProvider>()
                .AddScoped<IWriteStrategySelector, TopicStrategySelector>()
                .AddScoped<IWriteStrategySelector, ValueStrategySelector>()

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
                    {"Dotnet", Environment.Version.ToString()},
                    {"Library", Library.VersionString},
                    {"Connect", Assembly.GetExecutingAssembly().GetName().Version?.ToString()},
                    {"Application", Environment.GetEnvironmentVariable("APPLICATION_VERSION") ?? "0.0.0.0"}
                })
                .Enrich.WithProperty("Environment", Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"))
                .Enrich.WithProperty("Worker",
                    Environment.GetEnvironmentVariable("WORKER_HOST") ?? Environment.MachineName);
        }
    }
}