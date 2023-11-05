using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers;

public interface ISourceProcessor
{
    Task<IList<CommandContext>> Process(ConnectRecordBatch batch, string connector);
    Task<Message<byte[], byte[]>> GetCommandMessage(CommandContext context);
}