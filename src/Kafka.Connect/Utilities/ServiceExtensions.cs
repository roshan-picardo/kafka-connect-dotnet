using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
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
using ConfigurationProvider = Kafka.Connect.Configurations.ConfigurationProvider;
using IConfigurationProvider = Kafka.Connect.Configurations.IConfigurationProvider;
using WorkerConfig = Kafka.Connect.Config.WorkerConfig;

namespace Kafka.Connect.Utilities
{
    internal static class ServiceExtensions
    {
        internal static Action<IServiceCollection> AddPluginServices;

        internal static IServiceCollection AddServices(this IServiceCollection services, IConfiguration configuration)
        {
            services
                .AddScoped<IConnector, Connector>()
                .AddScoped<ISinkTask, SinkTask>()
                .AddScoped<IKafkaClientBuilder, KafkaClientBuilder>()
                .AddScoped<IRetriableHandler, RetriableHandler>()

                .AddScoped<IConnectDeadLetter, ConnectDeadLetter>()
                .AddScoped<ITokenHandler, TokenHandler>()

                .AddScoped<IProcessorServiceProvider, ProcessorServiceProvider>()
                .AddScoped<ISinkHandlerProvider, SinkHandlerProvider>()

                .AddScoped<IGenericRecordParser, GenericRecordParser>()
                .AddScoped<IRecordFlattener, JsonRecordFlattener>()
                .AddScoped<IMessageHandler, MessageHandler>()
                .AddScoped<ISinkConsumer, SinkConsumer>()
                .AddScoped<ISinkProcessor, SinkProcessor>()
                .AddScoped<IPartitionHandler, PartitionHandler>()
                .AddScoped<ISinkExceptionHandler, SinkExceptionHandler>()

                .AddScoped<IProcessor, JsonTypeOverrider>()
                .AddScoped<IProcessor, DateTimeTypeOverrider>()
                .AddScoped<IProcessor, BlacklistFieldProjector>()
                .AddScoped<IProcessor, WhitelistFieldProjector>()
                .AddScoped<IProcessor, FieldRenamer>()
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
                .AddScoped<IDeserializer, AvroDeserializer>()
                .AddScoped<IDeserializer, JsonDeserializer>()
                .AddScoped<IDeserializer, JsonSchemaDeserializer>()
                .AddScoped<IDeserializer, StringDeserializer>()
                .AddScoped<IDeserializer, IgnoreDeserializer>()
                .AddScoped<IMessageConverter, MessageConverter>()

                //.Configure<SchemaRegistryConfig>(configuration.GetSection("worker:schemaRegistry"))
                .Configure<WorkerConfig>(configuration.GetSection("worker"))
                .Configure<Kafka.Connect.Configurations.WorkerConfig>(configuration.GetSection("worker"))
                
                .Configure<List<ConnectorConfig<IDictionary<string, string>>>>(configuration.GetSection("worker:connectors"))
                .Configure<List<ConnectorConfig<IList<string>>>>(configuration.GetSection("worker:connectors"))
                .Configure<ConnectorConfig<IDictionary<string, string>>>(configuration.GetSection("worker:shared"))
                .Configure<ConnectorConfig<IList<string>>>(configuration.GetSection("worker:shared"))
                
                .AddSingleton<IConfigurationProvider, ConfigurationProvider>()
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