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
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Processors;
using Kafka.Connect.Serializers;
using Kafka.Connect.Plugin.Processors;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Kafka.Connect.Schemas;
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
            services
                .AddSingleton<ILogDecorator, LogDecorator>()
                .AddScoped<IConnector, Connector>()
                .AddScoped<ISinkTask, SinkTask>()
                .AddScopedWithLogging<IKafkaClientBuilder, KafkaClientBuilder>()
                .AddScoped<IKafkaClientEventHandler, KafkaClientEventHandler>()
                .AddScoped<IRetriableHandler, RetriableHandler>()

                .AddScopedWithLogging<IConnectDeadLetter, ConnectDeadLetter>()
                .AddScoped<ITokenHandler, TokenHandler>()

                .AddScoped<IProcessorServiceProvider, ProcessorServiceProvider>()
                .AddScoped<ISinkHandlerProvider, SinkHandlerProvider>()

                .AddScopedWithLogging<IGenericRecordParser, GenericRecordParser>()
                .AddScopedWithLogging<IRecordFlattener, JsonRecordFlattener>()
                .AddScopedWithLogging<IMessageHandler, MessageHandler>()
                .AddScopedWithLogging<ISinkConsumer, SinkConsumer>()
                .AddScopedWithLogging<ISinkProcessor, SinkProcessor>()
                .AddScopedWithLogging<IPartitionHandler, PartitionHandler>()
                .AddScopedWithLogging<ISinkExceptionHandler, SinkExceptionHandler>()

                .AddScopedWithLogging<IProcessor, JsonTypeOverrider>()
                .AddScopedWithLogging<IProcessor, DateTimeTypeOverrider>()
                .AddScopedWithLogging<IProcessor, BlacklistFieldProjector>()
                .AddScopedWithLogging<IProcessor, WhitelistFieldProjector>()
                .AddScopedWithLogging<IProcessor, FieldRenamer>()
                
                .AddScoped<IAsyncDeserializer<GenericRecord>>(provider =>
                {
                    var client = provider.GetServices<ISchemaRegistryClient>()
                        .FirstOrDefault(c => c is ConsumerCachedSchemaRegistryClient);
                    return new AvroDeserializer<GenericRecord>(client);
                })
                .AddScoped<IAsyncSerializer<GenericRecord>>(provider =>
                {
                    var client = provider.GetServices<ISchemaRegistryClient>()
                        .FirstOrDefault(c => c is ProducerCachedSchemaRegistryClient);
                    return new AvroSerializer<GenericRecord>(client);
                })
                .AddScoped<IAsyncDeserializer<JObject>>(provider =>
                {
                    var client = provider.GetServices<ISchemaRegistryClient>()
                        .FirstOrDefault(c => c is ConsumerCachedSchemaRegistryClient);
                    return new AvroDeserializer<JObject>(client);
                })
                .AddScoped<ISchemaRegistryClient>(_ =>
                {
                    var config = configuration.GetSection("worker:schemaRegistry").Get<SchemaRegistryConfig>();
                    return string.IsNullOrEmpty(config?.Url) ? null : new ConsumerCachedSchemaRegistryClient(config);
                })
                .AddScoped<ISchemaRegistryClient>(_ =>
                {
                    var config = configuration.GetSection("worker:publisher:schemaRegistry")
                        .Get<SchemaRegistryConfig>() ?? configuration.GetSection("worker:schemaRegistry").Get<SchemaRegistryConfig>();
                    return string.IsNullOrEmpty(config?.Url) ? null : new ProducerCachedSchemaRegistryClient(config);
                })
                .AddScopedWithLogging<IDeserializer, AvroDeserializer>()
                .AddScopedWithLogging<IDeserializer, JsonDeserializer>()
                .AddScopedWithLogging<IDeserializer, JsonSchemaDeserializer>()
                .AddScopedWithLogging<IDeserializer, StringDeserializer>()
                .AddScopedWithLogging<IDeserializer, IgnoreDeserializer>()
                .AddScopedWithLogging<IMessageConverter, MessageConverter>()

                //.Configure<SchemaRegistryConfig>(configuration.GetSection("worker:schemaRegistry"))
                .Configure<WorkerConfig>(configuration.GetSection("worker"))
                
                .Configure<List<ConnectorConfig<IDictionary<string, string>>>>(configuration.GetSection("worker:connectors"))
                .Configure<List<ConnectorConfig<IList<string>>>>(configuration.GetSection("worker:connectors"))
                .Configure<ConnectorConfig<IDictionary<string, string>>>(configuration.GetSection("worker:shared"))
                .Configure<ConnectorConfig<IList<string>>>(configuration.GetSection("worker:shared"))
                
                .AddSingleton<Providers.IConfigurationProvider, Providers.ConfigurationProvider>()
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
                    {"Librdkafka", Library.VersionString},
                    {"Connector", Assembly.GetExecutingAssembly().GetName().Version?.ToString()},
                    {"Application", Environment.GetEnvironmentVariable("APPLICATION_VERSION") ?? "0.0.0.0"}
                })
                .Enrich.WithProperty("Environment", Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"))
                .Enrich.WithProperty("Worker",
                    Environment.GetEnvironmentVariable("WORKER_HOST") ?? Environment.MachineName);
        }
    }
}