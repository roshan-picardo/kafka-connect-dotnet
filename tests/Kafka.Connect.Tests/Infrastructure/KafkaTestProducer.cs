using Confluent.Kafka;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Tests.Infrastructure;

public class KafkaTestProducer : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaTestProducer> _logger;

    public KafkaTestProducer(string bootstrapServers, ILogger<KafkaTestProducer> logger)
    {
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            RetryBackoffMs = 1000,
            MessageSendMaxRetries = 3,
            RequestTimeoutMs = 30000,
            EnableIdempotence = true
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Producer error: {Error}", e.Reason))
            .SetLogHandler((_, log) => _logger.LogDebug("Producer log: {Message}", log.Message))
            .Build();
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync(string topic, string key, object value, Dictionary<string, string>? headers = null)
    {
        var jsonValue = JsonSerializer.Serialize(value, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        return await ProduceAsync(topic, key, jsonValue, headers);
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync(string topic, string key, string value, Dictionary<string, string>? headers = null)
    {
        var message = new Message<string, string>
        {
            Key = key,
            Value = value
        };

        if (headers != null)
        {
            message.Headers = new Headers();
            foreach (var header in headers)
            {
                message.Headers.Add(header.Key, System.Text.Encoding.UTF8.GetBytes(header.Value));
            }
        }

        try
        {
            var deliveryResult = await _producer.ProduceAsync(topic, message);
            _logger.LogInformation("Produced message to {Topic}:{Partition}:{Offset} with key: {Key}", 
                deliveryResult.Topic, deliveryResult.Partition, deliveryResult.Offset, key);
            return deliveryResult;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, "Failed to produce message to topic {Topic} with key {Key}: {Error}", 
                topic, key, ex.Error.Reason);
            throw;
        }
    }

    public async Task<List<DeliveryResult<string, string>>> ProduceBatchAsync(string topic, IEnumerable<(string key, object value)> messages, Dictionary<string, string>? commonHeaders = null)
    {
        var results = new List<DeliveryResult<string, string>>();
        var tasks = new List<Task<DeliveryResult<string, string>>>();

        foreach (var (key, value) in messages)
        {
            tasks.Add(ProduceAsync(topic, key, value, commonHeaders));
        }

        var deliveryResults = await Task.WhenAll(tasks);
        results.AddRange(deliveryResults);

        _logger.LogInformation("Produced batch of {Count} messages to topic {Topic}", results.Count, topic);
        return results;
    }

    public async Task ProduceChangeDataCaptureMessageAsync(string topic, string key, object? before, object? after, string operation = "u")
    {
        var cdcMessage = new
        {
            before = before,
            after = after,
            op = operation,
            ts_ms = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            source = new
            {
                version = "1.0.0",
                connector = "test-connector",
                name = "test-source",
                ts_ms = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                snapshot = "false",
                db = "kafka_connect_test",
                table = "test_customers"
            }
        };

        await ProduceAsync(topic, key, cdcMessage);
    }

    public async Task ProduceInsertMessageAsync(string topic, string key, object record)
    {
        await ProduceChangeDataCaptureMessageAsync(topic, key, null, record, "c");
    }

    public async Task ProduceUpdateMessageAsync(string topic, string key, object beforeRecord, object afterRecord)
    {
        await ProduceChangeDataCaptureMessageAsync(topic, key, beforeRecord, afterRecord, "u");
    }

    public async Task ProduceDeleteMessageAsync(string topic, string key, object record)
    {
        await ProduceChangeDataCaptureMessageAsync(topic, key, record, null, "d");
    }

    public void Flush(TimeSpan timeout)
    {
        _producer.Flush(timeout);
        _logger.LogInformation("Producer flushed with timeout: {Timeout}", timeout);
    }

    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
    }
}

public static class KafkaTestProducerExtensions
{
    public static async Task ProduceTestCustomerAsync(this KafkaTestProducer producer, string topic, int id, string name, string email, int age)
    {
        var customer = new
        {
            id = id,
            name = name,
            email = email,
            age = age,
            created_at = DateTime.UtcNow,
            updated_at = DateTime.UtcNow
        };

        await producer.ProduceAsync(topic, id.ToString(), customer);
    }

    public static async Task ProduceTestCustomerInsertAsync(this KafkaTestProducer producer, string topic, int id, string name, string email, int age)
    {
        var customer = new
        {
            id = id,
            name = name,
            email = email,
            age = age,
            created_at = DateTime.UtcNow,
            updated_at = DateTime.UtcNow
        };

        await producer.ProduceInsertMessageAsync(topic, id.ToString(), customer);
    }

    public static async Task ProduceTestCustomerUpdateAsync(this KafkaTestProducer producer, string topic, int id, 
        string oldName, string oldEmail, int oldAge,
        string newName, string newEmail, int newAge)
    {
        var beforeCustomer = new
        {
            id = id,
            name = oldName,
            email = oldEmail,
            age = oldAge,
            created_at = DateTime.UtcNow.AddDays(-1),
            updated_at = DateTime.UtcNow.AddDays(-1)
        };

        var afterCustomer = new
        {
            id = id,
            name = newName,
            email = newEmail,
            age = newAge,
            created_at = DateTime.UtcNow.AddDays(-1),
            updated_at = DateTime.UtcNow
        };

        await producer.ProduceUpdateMessageAsync(topic, id.ToString(), beforeCustomer, afterCustomer);
    }

    public static async Task ProduceTestCustomerDeleteAsync(this KafkaTestProducer producer, string topic, int id, string name, string email, int age)
    {
        var customer = new
        {
            id = id,
            name = name,
            email = email,
            age = age,
            created_at = DateTime.UtcNow.AddDays(-1),
            updated_at = DateTime.UtcNow.AddDays(-1)
        };

        await producer.ProduceDeleteMessageAsync(topic, id.ToString(), customer);
    }
}