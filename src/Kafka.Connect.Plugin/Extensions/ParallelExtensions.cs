using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Extensions;

public static class ParallelExtensions
{
    public static async Task ForEachAsync(
        this IEnumerable<IConnectRecord> records,
        ParallelRetryOptions parallelRetryOptions,
        Func<IConnectRecord, Task> body)
    {
        var connectRecords = records.TakeWhile(record => record.Status != Status.Aborted).ToArray();
        var attempts = parallelRetryOptions.Attempts;
        do
        {
            attempts--;
            await Parallel.ForEachAsync(connectRecords,
                new ParallelOptions { MaxDegreeOfParallelism = parallelRetryOptions.DegreeOfParallelism },
                async (record, _) =>
                {
                    try
                    {
                        record.Exception = null;
                        await body(record);
                    }
                    catch (Exception ex)
                    {
                        var handleEx = ex.InnerException ?? ex;
                        record.Exception = handleEx is ConnectException
                            ? handleEx
                            : new ConnectDataException(handleEx.Message, handleEx);
                        record.Status = handleEx is ConnectRetriableException ? Status.Retrying : Status.Failed;
                    }
                });
        } while (connectRecords.Any(r => r.Status == Status.Retrying) && attempts > 0);

        if (parallelRetryOptions.ErrorTolerance.None)
        {
            connectRecords = connectRecords.TakeUntil(record => record.Status is Status.Retrying or Status.Failed).ToArray();
        }
        else if (parallelRetryOptions.ErrorTolerance.Data)
        {
            connectRecords = connectRecords.TakeUntil(record => record.Status is Status.Retrying).ToArray();
        }

        connectRecords.ForEach(record =>
        {
            if ((parallelRetryOptions.ErrorTolerance.None && record.Status is Status.Retrying or Status.Failed) ||
                parallelRetryOptions.ErrorTolerance.Data && record.Status == Status.Retrying)
            {
                record.Status = Status.Aborted;
            }
            else if (record.Status == Status.Retrying)
            {
                record.Status = Status.Failed;
            }
        });
    }

    public static void ForEach<T>(this IEnumerable<T> source, Action<T> body)
    {
        foreach (var item in source)
        {
            body(item);
        }
    }

    private static IEnumerable<T> TakeUntil<T>(this IEnumerable<T> source, Func<T, bool> predicate )
    {
        foreach ( var item in source )
        {
            yield return item;

            if (predicate(item))
            {
                yield break;
            }
        }
    }
}
