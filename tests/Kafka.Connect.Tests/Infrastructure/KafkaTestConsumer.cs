using Confluent.Kafka;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Tests.Infrastructure;

public class KafkaTestConsumer : IDisposable
{
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger<KafkaTestConsumer> _logger;
    private readonly CancellationTokenSource _cancellationTokenSource;

    public KafkaTestConsumer(string bootstrapServers, string groupId, ILogger<KafkaTestConsumer> logger)
    {
        _logger = logger;
        _cancellationTokenSource = new CancellationTokenSource();

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            SessionTimeoutMs = 6000,
            EnablePartitionEof = true
        };

        _consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Consumer error: {Error}", e.Reason))
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogInformation("Assigned partitions: [{Partitions}]", 
                    string.Join(", ", partitions.Select(p => $"{p.Topic}:{p.Partition}")));
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                _logger.LogInformation("Revoked partitions: [{Partitions}]", 
                    string.Join(", ", partitions.Select(p => $"{p.Topic}:{p.Partition}")));
            })
            .Build();
    }

    public async Task<List<ConsumedMessage>> ConsumeMessagesAsync(string topic, int expectedMessageCount, TimeSpan timeout)
    {
        var messages = new List<ConsumedMessage>();
        var endTime = DateTime.UtcNow.Add(timeout);

        _consumer.Subscribe(topic);
        _logger.LogInformation("Subscribed to topic: {Topic}, expecting {Count} messages", topic, expectedMessageCount);

        try
        {
            while (messages.Count < expectedMessageCount && DateTime.UtcNow < endTime && !_cancellationTokenSource.Token.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));
                    
                    if (consumeResult == null)
                        continue;

                    if (consumeResult.IsPartitionEOF)
                    {
                        _logger.LogDebug("Reached end of partition {Partition} on topic {Topic}", 
                            consumeResult.Partition, consumeResult.Topic);
                        continue;
                    }

                    var message = new ConsumedMessage
                    {
                        Topic = consumeResult.Topic,
                        Partition = consumeResult.Partition.Value,
                        Offset = consumeResult.Offset.Value,
                        Key = consumeResult.Message.Key,
                        Value = consumeResult.Message.Value,
                        Timestamp = consumeResult.Message.Timestamp.UtcDateTime,
                        Headers = consumeResult.Message.Headers?.ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes())) ?? new Dictionary<string, string>()
                    };

                    messages.Add(message);
                    _logger.LogInformation("Consumed message {Count}/{Expected} from {Topic}:{Partition}:{Offset}", 
                        messages.Count, expectedMessageCount, consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);

                    _consumer.Commit(consumeResult);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Error consuming message: {Error}", ex.Error.Reason);
                }
            }
        }
        finally
        {
            _consumer.Unsubscribe();
        }

        _logger.LogInformation("Consumed {ActualCount} out of {ExpectedCount} messages from topic {Topic}", 
            messages.Count, expectedMessageCount, topic);

        return messages;
    }

    public async Task<List<ConsumedMessage>> ConsumeAllAvailableMessagesAsync(string topic, TimeSpan timeout)
    {
        var messages = new List<ConsumedMessage>();
        var endTime = DateTime.UtcNow.Add(timeout);
        var noMessageTimeout = TimeSpan.FromSeconds(5);
        var lastMessageTime = DateTime.UtcNow;

        _consumer.Subscribe(topic);
        _logger.LogInformation("Subscribed to topic: {Topic}, consuming all available messages", topic);

        try
        {
            while (DateTime.UtcNow < endTime && !_cancellationTokenSource.Token.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));
                    
                    if (consumeResult == null)
                    {
                        // Check if we haven't received a message for the no-message timeout period
                        if (DateTime.UtcNow - lastMessageTime > noMessageTimeout)
                        {
                            _logger.LogInformation("No messages received for {Timeout} seconds, stopping consumption", 
                                noMessageTimeout.TotalSeconds);
                            break;
                        }
                        continue;
                    }

                    if (consumeResult.IsPartitionEOF)
                    {
                        _logger.LogDebug("Reached end of partition {Partition} on topic {Topic}", 
                            consumeResult.Partition, consumeResult.Topic);
                        continue;
                    }

                    lastMessageTime = DateTime.UtcNow;

                    var message = new ConsumedMessage
                    {
                        Topic = consumeResult.Topic,
                        Partition = consumeResult.Partition.Value,
                        Offset = consumeResult.Offset.Value,
                        Key = consumeResult.Message.Key,
                        Value = consumeResult.Message.Value,
                        Timestamp = consumeResult.Message.Timestamp.UtcDateTime,
                        Headers = consumeResult.Message.Headers?.ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes())) ?? new Dictionary<string, string>()
                    };

                    messages.Add(message);
                    _logger.LogInformation("Consumed message {Count} from {Topic}:{Partition}:{Offset}", 
                        messages.Count, consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);

                    _consumer.Commit(consumeResult);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Error consuming message: {Error}", ex.Error.Reason);
                }
            }
        }
        finally
        {
            _consumer.Unsubscribe();
        }

        _logger.LogInformation("Consumed {Count} messages from topic {Topic}", messages.Count, topic);
        return messages;
    }

    public void Stop()
    {
        _cancellationTokenSource.Cancel();
    }

    public void Dispose()
    {
        _cancellationTokenSource?.Cancel();
        _consumer?.Close();
        _consumer?.Dispose();
        _cancellationTokenSource?.Dispose();
    }
}

public class ConsumedMessage
{
    public string Topic { get; set; } = string.Empty;
    public int Partition { get; set; }
    public long Offset { get; set; }
    public string? Key { get; set; }
    public string? Value { get; set; }
    public DateTime Timestamp { get; set; }
    public Dictionary<string, string> Headers { get; set; } = new();

    public T? DeserializeValue<T>() where T : class
    {
        if (string.IsNullOrEmpty(Value))
            return null;

        try
        {
            return JsonSerializer.Deserialize<T>(Value, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to deserialize message value to {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public T? DeserializeKey<T>() where T : class
    {
        if (string.IsNullOrEmpty(Key))
            return null;

        try
        {
            return JsonSerializer.Deserialize<T>(Key, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Failed to deserialize message key to {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public override string ToString()
    {
        return $"Message[{Topic}:{Partition}:{Offset}] Key={Key}, Value={Value}, Timestamp={Timestamp:yyyy-MM-dd HH:mm:ss}";
    }
}