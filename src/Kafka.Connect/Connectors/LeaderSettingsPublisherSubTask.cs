using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Connectors;

public class LeaderSettingsPublisherSubTask(
    IExecutionContext executionContext,
    IConnectRecordCollection leaderRecordCollection,
    ILogger<LeaderSettingsPublisherSubTask> logger)
    : ILeaderSubTask
{
    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        logger.Info($"Staring leader task: {connector},  taskId: {taskId:00}");
        executionContext.Initialize(connector, taskId, this);
        await leaderRecordCollection.Setup(ConnectorType.Leader, connector, taskId);
        
        if (!leaderRecordCollection.TryPublisher())
        {
            IsStopped = true;
            return;
        }

        executionContext.UpdateLeaderAssignments(connector, taskId, TopicType.Config);

        var reader = executionContext.ConfigurationChannel.Reader;
        
        while (!cts.IsCancellationRequested)
        {
            try
            {
                var configuration = await reader.ReadAsync(cts.Token);
                leaderRecordCollection.Configure(connector, configuration);
                await leaderRecordCollection.Process(connector);
                await leaderRecordCollection.Produce(connector);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.Error("Publisher loop exception", ex);
            }
            finally
            {
                leaderRecordCollection.Record(connector);
            }
        }

        leaderRecordCollection.Cleanup();
        IsStopped = true;
    }

    public bool IsPaused => false;
    public bool IsStopped { get; private set; }
}
