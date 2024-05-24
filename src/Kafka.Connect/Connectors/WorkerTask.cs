using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Connectors;

public class WorkerTask(
    IAdminClient adminClient,
    IConfigurationProvider configurationProvider,
    IExecutionContext executionContext,
    IConnectRecordCollection workerRecordCollection)
    : IWorkerTask
{
    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        executionContext.Initialize(connector, taskId, this);
        
        var groupId = configurationProvider.GetGroupId(connector);
        var groups =
            (await adminClient.DescribeConsumerGroupsAsync(new[] { groupId })).ConsumerGroupDescriptions.FirstOrDefault();
        if (!(groups?.Members == null || groups.Members.Count == 0))
        {
            workerRecordCollection.Setup(ConnectorType.Worker, connector, taskId);
            if (workerRecordCollection.TrySubscribe())
            {
                await workerRecordCollection.Consume(cts.Token);
                
            }
            // create consumer
        }
    }

    public bool IsPaused { get; private set; }
    public bool IsStopped { get; private set; }
}