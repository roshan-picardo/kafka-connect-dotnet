using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace Kafka.Connect.Tests.Infrastructure;

public class KafkaTestConsumer : IDisposable
{
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger _logger;
    private bool _disposed;

    public KafkaTestConsumer(string bootstrapServers, ILogger logger)
    {
        _logger = logger;
        
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid()}",
            ClientId = $"test-consumer-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            SessionTimeoutMs = 6000,
            MaxPollIntervalMs = 300000,
            EnablePartitionEof = true
        };

        _consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Consumer error: {Error}", e.Reason))
            .SetLogHandler((_, log) => _logger.LogDebug("Consumer log: {Message}", log.Message))
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogDebug("Assigned partitions: [{Partitions}]", 
                    string.Join(", ", partitions.Select(p => $"{p.Topic}:{p.Partition}")));
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                _logger.LogDebug("Revoked partitions: [{Partitions}]", 
                    string.Join(", ", partitions.Select(p => $"{p.Topic}:{p.Partition}")));
            })
            .Build();
    }

    public void Subscribe(string topic)
    {
        Subscribe(new[] { topic });
    }

    public void Subscribe(IEnumerable<string> topics)
    {
        var topicList = topics.ToList();
        _logger.LogDebug("Subscribing to topics: [{Topics}]", string.Join(", ", topicList));
        _consumer.Subscribe(topicList);
    }

    public ConsumeResult<string, string>? Consume(TimeSpan timeout)
    {
        try
        {
            var result = _consumer.Consume(timeout);
            
            if (result != null && !result.IsPartitionEOF)
            {
                _logger.LogDebug("Consumed message from {Topic}:{Partition}:{Offset} with key {Key}", 
                    result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Key);
            }
            
            return result;
        }
        catch (ConsumeException ex)
        {
            _logger.LogError(ex, "Error consuming message");
            throw;
        }
    }

    public async Task<ConsumeResult<string, string>?> ConsumeAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        return await Task.Run(() => Consume(timeout), cancellationToken);
    }

    public async Task<List<ConsumeResult<string, string>>> ConsumeMessagesAsync(
        int expectedCount, 
        TimeSpan timeout, 
        CancellationToken cancellationToken = default)
    {
        var messages = new List<ConsumeResult<string, string>>();
        var endTime = DateTime.UtcNow.Add(timeout);

        while (messages.Count < expectedCount && DateTime.UtcNow < endTime && !cancellationToken.IsCancellationRequested)
        {
            var remainingTime = endTime - DateTime.UtcNow;
            if (remainingTime <= TimeSpan.Zero) break;

            var result = await ConsumeAsync(TimeSpan.FromSeconds(1), cancellationToken);
            
            if (result != null && !result.IsPartitionEOF)
            {
                messages.Add(result);
            }
        }

        _logger.LogDebug("Consumed {ActualCount}/{ExpectedCount} messages", messages.Count, expectedCount);
        return messages;
    }

    public async Task<List<T>> ConsumeJsonMessagesAsync<T>(
        int expectedCount, 
        TimeSpan timeout, 
        CancellationToken cancellationToken = default)
    {
        var messages = await ConsumeMessagesAsync(expectedCount, timeout, cancellationToken);
        var results = new List<T>();

        foreach (var message in messages)
        {
            try
            {
                var obj = JsonSerializer.Deserialize<T>(message.Message.Value, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
                
                if (obj != null)
                {
                    results.Add(obj);
                }
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "Failed to deserialize message: {Value}", message.Message.Value);
            }
        }

        return results;
    }

    public async Task<ConsumeResult<string, string>?> WaitForMessageAsync(
        Func<ConsumeResult<string, string>, bool> predicate,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        var endTime = DateTime.UtcNow.Add(timeout);

        while (DateTime.UtcNow < endTime && !cancellationToken.IsCancellationRequested)
        {
            var remainingTime = endTime - DateTime.UtcNow;
            if (remainingTime <= TimeSpan.Zero) break;

            var result = await ConsumeAsync(TimeSpan.FromSeconds(1), cancellationToken);
            
            if (result != null && !result.IsPartitionEOF && predicate(result))
            {
                return result;
            }
        }

        return null;
    }

    public void Commit()
    {
        try
        {
            _consumer.Commit();
            _logger.LogDebug("Committed consumer offsets");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to commit consumer offsets");
            throw;
        }
    }

    public void Commit(ConsumeResult<string, string> result)
    {
        try
        {
            _consumer.Commit(result);
            _logger.LogDebug("Committed offset for {Topic}:{Partition}:{Offset}", 
                result.Topic, result.Partition.Value, result.Offset.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to commit offset for message");
            throw;
        }
    }

    public void Seek(TopicPartition topicPartition, Offset offset)
    {
        _consumer.Seek(new TopicPartitionOffset(topicPartition, offset));
        _logger.LogDebug("Seeked to {Topic}:{Partition}:{Offset}",
            topicPartition.Topic, topicPartition.Partition.Value, offset.Value);
    }

    public void Close()
    {
        _consumer.Close();
        _logger.LogDebug("Consumer closed");
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        try
        {
            _consumer?.Close();
            _consumer?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disposing Kafka consumer");
        }
        
        _disposed = true;
    }
}