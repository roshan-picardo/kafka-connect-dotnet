using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Connect.Converters;
using Kafka.Connect.FunctionalTests.Targets;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.FunctionalTests;

public class Fixture : IDisposable
{
    private IProducer<Null, GenericRecord> _keyNullProducer;
    private IProducer<string, GenericRecord> _keyStringProducer;
    private IProducer<GenericRecord, GenericRecord> _keyGenericProducer;
    private readonly TargetHelperProvider _targetHelperProvider;
    private readonly GenericRecordBuilder _genericRecordBuilder;

    private readonly InitConfig _settings; 

    public Fixture()
    {
        _settings = InitConfig.Get();
        ConfigureMessageProducers();
        _targetHelperProvider = new TargetHelperProvider(_settings);
        _genericRecordBuilder = new GenericRecordBuilder();
    }

    public Task Setup(Sink sink)
    {
        return _targetHelperProvider.GetHelper(sink.Type).Setup(sink);
    }
    
    public Task<(bool, string)> Validate(Sink sink)
    {
        return _targetHelperProvider.GetHelper(sink.Type).Validate(sink);
    }
    
    public Task Cleanup(Sink sink)
    {
        return _targetHelperProvider.GetHelper(sink.Type).Cleanup(sink);
    }
    
    public async Task Send(string topic, dynamic schema, IEnumerable<Record> messages)
    {
        foreach (var message in messages)
        {
            var schemaValue = (RecordSchema) Avro.Schema.Parse(schema.Value?.ToString());
            var genericRecord = _genericRecordBuilder.Build(schemaValue, message.Value);

            TopicPartitionOffset delivered;
            if (schema.Key == null)
            {
                delivered = (await _keyNullProducer.ProduceAsync(topic,
                        new Message<Null, GenericRecord> {Key = null, Value = genericRecord}))
                    .TopicPartitionOffset;
            }
            else if (schema.Key is string)
            {
                delivered = (await _keyStringProducer.ProduceAsync(topic,
                        new Message<string, GenericRecord>
                            {Key = message.Key.ToString(), Value = genericRecord}))
                    .TopicPartitionOffset;
            }
            else
            {
                var schemaKey = (RecordSchema) Avro.Schema.Parse(schema.Key?.ToString());
                var keyRecord = _genericRecordBuilder.Build(schemaKey, message.Key);
                delivered = (await _keyGenericProducer.ProduceAsync(topic,
                        new Message<GenericRecord, GenericRecord>
                            {Key = keyRecord, Value = genericRecord}))
                    .TopicPartitionOffset;
            }
            Console.WriteLine($"{DateTime.Now} : {delivered.Topic} : {delivered.Partition.Value:00} - {delivered.Offset.Value:0000}");
        }
    }

    private void ConfigureMessageProducers()
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers
        };

        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = _settings.SchemaRegistryUrl
        };
        var avroSerializerConfig = new AvroSerializerConfig
        {
            BufferBytes = 100,
            AutoRegisterSchemas = true,
        };
        
        var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        _keyNullProducer = new ProducerBuilder<Null, GenericRecord>(producerConfig)
            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, avroSerializerConfig))
            .Build();

        _keyStringProducer = new ProducerBuilder<string, GenericRecord>(producerConfig)
            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, avroSerializerConfig))
            .Build();

        _keyGenericProducer = new ProducerBuilder<GenericRecord, GenericRecord>(producerConfig)
            .SetKeySerializer(new AvroSerializer<GenericRecord>(schemaRegistry, avroSerializerConfig))
            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, avroSerializerConfig))
            .Build();
    }
    
    public void Dispose()
    {
    }
}