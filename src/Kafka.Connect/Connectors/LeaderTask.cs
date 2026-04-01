using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Connect.Connectors;

public class LeaderTask(
    IEnumerable<ILeaderSubTask> subTasks)
    : ILeaderTask
{
    public async Task Execute(string connector, int taskId, CancellationTokenSource cts)
    {
        var subTasksList = subTasks.ToList();
        var subTask = subTasksList.ElementAtOrDefault(taskId - 1);
        
        if (subTask != null)
        {
            await subTask.Execute(connector, taskId, cts);
        }

        IsStopped = true;
    }

    public bool IsPaused => false;
    public bool IsStopped { get; private set; }
}
