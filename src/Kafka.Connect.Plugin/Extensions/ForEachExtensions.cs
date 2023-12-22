using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Exceptions;

namespace Kafka.Connect.Plugin.Extensions
{
    public static class ParallelEx
    {

        public static async Task ForEachAsync<T>(IEnumerable<T> source, ParallelOptions parallelOptions, Func<T, Task> body)
        {
            await Parallel.ForEachAsync(source, parallelOptions, async (s, _) =>
            {
                try
                {
                    await body(s);
                }
                catch (Exception ex)
                {
                    
                }
            });
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
}