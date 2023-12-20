using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers;

public class SourceProducer : ISourceProducer
{
    private readonly ILogger<SourceProducer> _logger;
    private readonly IKafkaClientBuilder _kafkaClientBuilder;
    private readonly IMessageHandler _messageHandler;

    public SourceProducer(
        ILogger<SourceProducer> logger,
        IKafkaClientBuilder kafkaClientBuilder, IMessageHandler messageHandler)
    {
        _logger = logger;
        _kafkaClientBuilder = kafkaClientBuilder;
        _messageHandler = messageHandler;
    }

    public IProducer<byte[], byte[]> GetProducer(string connector, int taskId)
    {
        using (_logger.Track("Generating Kafka producer"))
        {
            return _kafkaClientBuilder.GetProducer(connector);
        }
    }

    public async Task Produce(IProducer<byte[], byte[]> producer, string connector, int taskId, ConnectRecordBatch batch)
    {
        using (_logger.Track("Producing Kafka messages"))
        {
            await batch.ForEachAsync(record => Produce(producer, record.Topic, record.Serialized));
        }
    }

    public async Task Produce(IProducer<byte[], byte[]> producer, string topic, ConnectMessage<byte[]> message)
    {
        using (_logger.Track("Producing Kafka message"))
        {
            await producer.ProduceAsync(topic, new Message<byte[], byte[]> { Key = message.Key, Value = message.Value });
        }
    }

    public async Task Produce(IProducer<byte[], byte[]> producer,  CommandContext context)
    {
        var message = await _messageHandler.Serialize(context.Connector, context.Topic, new ConnectMessage<JsonNode>
        {
            Key = null,
            Value = System.Text.Json.JsonSerializer.SerializeToNode(context)
        });

        await producer.ProduceAsync(new TopicPartition(context.Topic, context.Partition),
            new Message<byte[], byte[]> { Key = message.Key, Value = message.Value });
    }
}