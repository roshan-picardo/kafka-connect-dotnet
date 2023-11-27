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

    public SourceProducer(
        ILogger<SourceProducer> logger,
        IKafkaClientBuilder kafkaClientBuilder)
    {
        _logger = logger;
        _kafkaClientBuilder = kafkaClientBuilder;
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
            await batch.ForEachAsync(record => producer.ProduceAsync(record.Topic,
                new Message<byte[], byte[]>()));
        }
    }

    public Task Produce(IProducer<byte[], byte[]> producer, string connector, int taskId, CommandContext command)
    {
        return Task.CompletedTask;
    }
}