using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers;

public interface ISourceProcessor
{
    Task<IList<CommandContext>> Commands(ConnectRecordBatch batch, string connector);
    Task Process(ConnectRecordBatch batch, string connector);
    Task<ConnectMessage<byte[]>> GetCommandMessage(CommandContext context);
}