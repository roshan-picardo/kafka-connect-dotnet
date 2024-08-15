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
        var connectRecords = records as IConnectRecord[] ?? records.ToArray();
        var attempts =  parallelRetryOptions.Attempts;
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
                        record.Status = handleEx is ConnectRetriableException && attempts > 0
                            ? Status.Retrying
                            : Status.Failed;
                    }
                });
        } while (connectRecords.Any(r => r.Status == Status.Retrying));

        if (!parallelRetryOptions.ErrorTolerated && connectRecords.Any(r => r.Status == Status.Failed))
        {
            var deferred = false;
            connectRecords.ForEach(record =>
            {
                record.Status = deferred ? Status.Deferred : record.Status;
                deferred = deferred || record.Status == Status.Failed;
            });
        }
    }

    public static void ForEach<T>(this IEnumerable<T> source, Action<T> body)
    {
        foreach (var item in source)
        {
            body(item);
        }
    }
}
