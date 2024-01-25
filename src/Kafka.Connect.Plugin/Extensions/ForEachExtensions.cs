using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin.Extensions;

public static class ParallelEx
{
    public static async Task ForEachAsync(
        IList<ConnectRecord> records,
        int degreeOfParallelism,
        Func<ConnectRecord, Task> body)
    {
        await Parallel.ForEachAsync(records,
            new ParallelOptions { MaxDegreeOfParallelism = degreeOfParallelism }, async (record, _) =>
            {
                try
                {
                    record.Exception = null;
                    await body(record);
                }
                catch (Exception ex)
                {
                    var handleEx = ex.InnerException ?? ex;
                    if (handleEx is ConnectException ce)
                    {
                        record.Exception = ce;
                    }
                    else
                    {
                        record.Exception = new ConnectDataException("Local_Fatal", handleEx);
                    }
                }
            });

        if (records.Any(r => r.Exception != null))
        {
            throw new ConnectAggregateException("Local_Application", false,
                records.Select(r => r.Exception).Where(e => e != null).ToArray());
        }
    }

    public static void ForEach(IEnumerable<ConnectRecord> records, Action<ConnectRecord> body)
    {
        foreach (var record in records)
        {
            body(record);
        }
    }
        
    public static async Task ForEach(IList<ConnectRecord> records, Func<ConnectRecord, Task> body)
    {
        foreach (var record in records)
        {
            try
            {
                record.Exception = null;
                await body(record);
            }
            catch (Exception ex)
            {
                var handleEx = ex.InnerException ?? ex;
                if (handleEx is ConnectException ce)
                {
                    record.Exception = ce;
                }
                else
                {
                    record.Exception = new ConnectDataException("Local_Fatal", handleEx);
                }
                break;
            }
        }

        if (records.Any(r => r.Exception != null))
        {
            throw new ConnectAggregateException("Local_Application", false,
                records.Select(r => r.Exception).Where(e => e != null).ToArray());
        }
    }
        
    public static async Task ForEach<T>(IList<ConnectRecord<T>> records, Func<ConnectRecord<T>, Task> body)
    {
        foreach (var record in records)
        {
            var connectRecord = record.GetRecord();
            try
            {
                connectRecord.Exception = null;
                await body(record);
            }
            catch (Exception ex)
            {
                var handleEx = ex.InnerException ?? ex;
                if (handleEx is ConnectException ce)
                {
                    connectRecord.Exception = ce;
                }
                else
                {
                    connectRecord.Exception = new ConnectDataException("Local_Fatal", handleEx);
                }
                break;
            }
        }

        if (records.Any(r => r.GetRecord().Exception != null))
        {
            throw new ConnectAggregateException("Local_Application", false,
                records.Select(r => r.GetRecord().Exception).Where(e => e != null).ToArray());
        }
    }

    public static async Task ForEachAsync<T>(
        this IEnumerable<T> source, Func<T, Task> body, Func<T, ConnectException, Exception> setLogContext = null,
        int dop = 100)
    {
        var exceptions = new ConcurrentBag<Exception>();
        var canRetry = false;

        void CatchException(Task task, T data)
        {
            if (!task.IsFaulted || task.Exception == null)
            {
                canRetry = true;
                return;
            }

            if (task.Exception.InnerException != null)
            {
                if (task.Exception.InnerException is ConnectException ce)
                {
                    exceptions.Add(setLogContext == null ? ce : setLogContext(data, ce));
                }
                else
                {
                    var cde = new ConnectDataException("Local_Fatal", task.Exception.InnerException);
                    exceptions.Add(setLogContext == null ? cde : setLogContext(data, cde));
                }
            }
            else if (task.Exception.InnerExceptions.Any())
            {
                var cae = new ConnectAggregateException("Local_Application", canRetry,
                    task.Exception.InnerExceptions.ToArray());
                exceptions.Add(setLogContext == null ? cae : setLogContext(data, cae));
            }
        }

        void CatchWhenAllException(Task task)
        {
            if (task.IsFaulted && task.Exception != null)
            {
                exceptions.Add(task.Exception);
            }
        }

        void ThrowException(Task _)
        {
            if (!exceptions.Any()) return;
            if (exceptions.Count == 1)
            {
                throw new ConnectAggregateException("Local_Application", exceptions.Single(), canRetry);
            }

            throw new ConnectAggregateException("Local_Application", canRetry, exceptions.ToArray());
        }

        await Task.WhenAll(
                from partition in Partitioner.Create(source).GetPartitions(dop)
                select Task.Run(async () =>
                {
                    using (partition)
                        while (partition.MoveNext())
                            await body(partition.Current)
                                .ContinueWith(t => CatchException(t, partition.Current))
                                .ConfigureAwait(false);

                }))
            .ContinueWith(CatchWhenAllException)
            .ContinueWith(ThrowException);
    }

    public static void ForEach<T>(this IEnumerable<T> source, Action<T> body)
    {
        foreach (var item in source)
        {
            body(item);
        }
    }
}