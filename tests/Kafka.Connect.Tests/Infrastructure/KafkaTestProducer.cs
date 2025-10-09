using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace Kafka.Connect.Tests.Infrastructure;

public class KafkaTestProducer : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger _logger;
    private bool _disposed;

    public KafkaTestProducer(string bootstrapServers, ILogger logger)
    {
        _logger = logger;
        
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = $"test-producer-{Guid.NewGuid()}",
            Acks = Acks.All,
            RetryBackoffMs = 1000,
            MessageTimeoutMs = 30000,
            EnableIdempotence = true
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Producer error: {Error}", e.Reason))
            .SetLogHandler((_, log) => _logger.LogDebug("Producer log: {Message}", log.Message))
            .Build();
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync(string topic, string key, string value)
    {
        try
        {
            _logger.LogDebug("Producing message to topic {Topic} with key {Key}", topic, key);
            
            var message = new Message<string, string>
            {
                Key = key,
                Value = value,
                Timestamp = new Timestamp(DateTime.UtcNow)
            };

            var result = await _producer.ProduceAsync(topic, message);
            
            _logger.LogDebug("Message produced successfully to {Topic}:{Partition}:{Offset}", 
                result.Topic, result.Partition.Value, result.Offset.Value);
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to produce message to topic {Topic}", topic);
            throw;
        }
    }

    public async Task<DeliveryResult<string, string>> ProduceJsonAsync<T>(string topic, string key, T value)
    {
        var json = JsonSerializer.Serialize(value, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
        
        return await ProduceAsync(topic, key, json);
    }

    public async Task ProduceBatchAsync(string topic, IEnumerable<KeyValuePair<string, string>> messages)
    {
        var tasks = messages.Select(msg => ProduceAsync(topic, msg.Key, msg.Value));
        await Task.WhenAll(tasks);
        
        // Ensure all messages are delivered
        _producer.Flush(TimeSpan.FromSeconds(30));
    }

    public async Task ProduceBatchJsonAsync<T>(string topic, IEnumerable<KeyValuePair<string, T>> messages)
    {
        var jsonMessages = messages.Select(msg => new KeyValuePair<string, string>(
            msg.Key,
            JsonSerializer.Serialize(msg.Value, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            })
        ));
        
        await ProduceBatchAsync(topic, jsonMessages);
    }

    public Metadata GetMetadata(string topic, TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(10);
        // GetMetadata method is not available in newer Kafka client versions
        // Use an alternative approach or remove this method if not essential
        throw new NotSupportedException("GetMetadata is not supported in this version of Confluent.Kafka");
    }

    public void Flush(TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(30);
        _producer.Flush(timeout.Value);
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        try
        {
            _producer?.Flush(TimeSpan.FromSeconds(10));
            _producer?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disposing Kafka producer");
        }
        
        _disposed = true;
    }
}